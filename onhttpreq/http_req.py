"""HTTP requester with caching for API requests"""

import http
import math
import time
import warnings
from datetime import datetime, timedelta
from typing import Callable, cast

import requests
import tqdm

from .cache import HTTPCache


class CacheOnlyError(Exception):
    """raised if cache_only is enabled and a url is not in the cache"""


class HTTPReqError(Exception):
    def __init__(self, http_response=None, msg=None, url=None):
        super().__init__(msg, http_response)
        self.http_resp = http_response
        self.msg = msg
        self.url = url

    def __str__(self):
        code = self.http_resp.status_code if self.http_resp is not None else None
        headers = self.http_resp.headers if self.http_resp is not None else None
        text = self.http_resp.text if self.http_resp is not None else None
        return f"""HTTPReqError for url='{self.url}'
msg '{self.msg}'
status code {code}
Headers:
{headers}
Content:
{text}"""


# TODO: these should be an enum
ON_RESPONSE_WAIT_RETRY = "wait_retry"
"""wait for a specified duration then try again"""
ON_RESPONSE_RETURN_WAIT = "return_wait"
"""return the http response then wait for a specified duration"""
ON_RESPONSE_FAIL = "fail"
"""raise a failure for the http request"""


# TODO: on_response should also take a URL arg
class HTTPReq:

    _tries: None | int

    def __init__(
        self,
        verbose=False,
        progress=False,
        http_retries=2,
        requests_kwargs=None,
        on_response: None | Callable = None,
        request_timeout=None,
        cache_filename=None,
        cache_in_memory=False,
        cache_overwrite=False,
        cache_dont_expire=False,
        compression=False,
        cache_only=False,
    ):
        """
        cache_in_memory: if true then create an in memory cache
        cache_only: results will only come from the cache. if a url is not available in the cache
          then an error occurs, when this is true nothing in the cache will be considered expired
        requests_kwargs: kwargs tp pass to requests when a get/request is made
        request_timeout: timeout in seconds for a request reqponse, if no response is received
          then a retry is attempted (if there are retries remaining)
        compression: compress the cache
        on_response: A callback that can be used to process the http request responses prior to
          returning results. Useful for handling header data that should result in varying the
          behavior of the cache, handling rate limits, etc.
          This should be a function that takes a request response, and returns None or
          a command in the form of a tuple (cmd, args_dict). If None is returned then no additional
          processing will be executed and the request response will be returned to the caller
          Available cmds and the arg_dict keys are

          ON_RESPONSE_WAIT_RETRY: dict with keys 'reason', 'duration' - Wait for duration seconds
            then repeat the request if this is used with progress then duration will be rounded
            up to the nearest second
          ON_RESPONSE_RETURN_WAIT: same as ON_RESPONSE_WAIT_RETRY except return the response
            and do not do the next request until after the wait period
          ON_RESPONSE_FAIL: dict with key 'reason' - Raise a failure exception, note that the
            entire response dict will be on the raised exception in case there is a need to
            pass through additional data
        """
        assert not (
            (cache_filename is not None) and cache_in_memory
        ), "caching can't both be in memory and to a file"
        assert not (
            cache_only and not cache_dont_expire
        ), "cache_dont_expire must be True if cache_only is True"

        self.cache_overwrite = cache_overwrite

        assert not (
            cache_filename is None and cache_only
        ), "cache_only + no cache_filename means there is no chance of getting results"
        self.cache_filename = cache_filename
        self.cache_only = cache_only

        self._cache = (
            HTTPCache(
                filename=cache_filename,
                verbose=verbose,
                dont_expire=cache_dont_expire,
                store_as_compressed=compression,
            )
            if (cache_filename is not None) or (cache_in_memory is True)
            else None
        )

        self._requests_kwargs = requests_kwargs or {}
        self._request_timeout = request_timeout
        self._retries = http_retries
        self.requests = 0
        self.requests_from_cache = 0
        self._return_wait_cmd: dict | None = None
        self.error_skips: list = []
        self.total_wait_secs = 0
        self.total_retries = 0
        self._on_response = on_response
        self.progress = progress
        self.verbose = verbose
        self.http_requests = 0

        self._last_result_details: None | dict = None

    @property
    def caching(self):
        return self._cache is not None

    @property
    def last_result_details(self):
        """return dict describing what happened during the last get"""
        return self._last_result_details

    @property
    def caching_enabled(self):
        return self._cache is not None

    def _wait(self, reason=None, duration=None, started_waiting_dt=None):
        assert duration is not None

        now = datetime.now()
        self.total_wait_secs += duration
        if started_waiting_dt is None:
            started_waiting_dt = now
        else:
            # the actual duration is possibly less due to execution latency
            wait_till_dt = started_waiting_dt + timedelta(seconds=duration)
            # pad with a second
            duration = (wait_till_dt - now).total_seconds()
        # wait an extra second for good measure
        if self.verbose or self.progress:
            if self.progress:
                for _ in tqdm.trange(
                    math.ceil(duration),
                    desc=reason or "waiting on rate limit",
                    leave=False,
                ):
                    time.sleep(1)

            else:
                msg = (
                    f"Rate limit reached, reason '{reason}'. Waiting {duration} "
                    f"seconds starting at {started_waiting_dt:%X}"
                )
                print("\n" + msg)
                # test for positive duration just in case testing or other processing causes latency
                if duration > 0:
                    time.sleep(duration)
        elif duration > 0:
            time.sleep(duration)

    def _process_on_response(self, get_response, url):
        """
        returns: true if the retry loop should be broken
        raises: ValueError if the on_response method returned an invalid result
        """
        assert self._on_response is not None
        res = self._on_response(get_response)
        if res is None:
            return True

        if res[0] == ON_RESPONSE_WAIT_RETRY:
            assert self._tries is not None
            if self._tries < self._retries + 1:
                # only makes sense to wait if there is another retry available
                self._wait(**res[1])
            return False

        if res[0] == ON_RESPONSE_RETURN_WAIT:
            assert isinstance(res[1], dict)
            self._return_wait_cmd = {"started_waiting_dt": datetime.now(), **res[1]}
            return True

        if res[0] == ON_RESPONSE_FAIL:
            raise HTTPReqError(http_response=get_response, msg=res[1], url=url)

        raise ValueError(f"on_response callback returned an unknown command. {res}")

    def set_cached_expiration(self, url, **expiration):
        """
        for kwargs see cache set_expiration
        """
        if self._cache:
            self._cache.set_expiration(url, **expiration)
        else:
            warnings.warn(
                f"Attempted to expire '{url}' from cache, but caching is not currently enabled."
            )

    @property
    def history(self):
        """return a dict describing the request history"""
        return {
            "requests": self.requests,
            "requests_from_http": self.http_requests,
            "requests_from_cache": self.requests_from_cache,
            "wait_secs": self.total_wait_secs,
            "error_skips": self.error_skips,
            "request_retries": self.total_retries,
        }

    _GetReturnType = int | float | dict | str | list | bytes

    def get(
        self,
        url: str,
        parse_json=True,
        cache_fail_func=None,
        skip_cache=False,
        cache_key: str | None = None,
    ) -> _GetReturnType:
        """
        cache_fail_func: if cache is enabled and the url is not in the cache and this is not None
           then call this func. Useful for displaying messages
        cache_key: if not None then caching of the request content will use this key instead
           of the url. if None then cache storage/lookup will be against the url
        """
        assert not (skip_cache and self.cache_only)

        self._last_result_details = {"url": url, "http_attempts": 0}

        if self._return_wait_cmd is not None:
            self._wait(**self._return_wait_cmd)
            self._return_wait_cmd = None
        self.requests += 1
        if self.verbose:
            print(f"\nHTTP request: '{url}' : '{self._requests_kwargs}'\n")

        result = None
        url_for_cache_key = cache_key or url
        if self._cache is not None and not self.cache_overwrite and not skip_cache:
            result = (
                self._cache.get_json(url_for_cache_key)
                if parse_json
                else self._cache.get(url_for_cache_key)
            )
            if self.verbose:
                print(("not " if result is None else "") + "found in cache")
            if result is not None:
                self._last_result_details["retrieved_from"] = "cache"
                self.requests_from_cache += 1
                return cast(HTTPReq._GetReturnType, result)

        if self.cache_only:
            raise CacheOnlyError(f"'{url_for_cache_key}' not in cache")

        # cache search failed
        if cache_fail_func is not None:
            cache_fail_func()

        self._tries = 0
        while self._tries < self._retries + 1:
            self._tries += 1
            self.http_requests += 1
            try:
                self._last_result_details["http_attempts"] += 1
                r = requests.get(url=url, timeout=self._request_timeout, **self._requests_kwargs)
            except requests.exceptions.Timeout as ex:
                r = None
                if self.verbose:
                    print(f"HTTPReq request timed out... {ex}")

            if self.verbose and r is not None:
                print(
                    f"HTTPReq response for attempt {self._tries + 1}/{self._retries} "
                    f"code: {r.status_code}"
                )
                print(f"HTTPReq Headers: {r.headers}")
                print()
                print(r.text)

            if self._on_response is not None:
                if self._process_on_response(r, url):
                    break
            elif r is not None and r.status_code == http.client.OK:
                break

            if self.verbose:
                print(f"Retry #{self._tries + 1}")

        self.total_retries += max(0, self._tries - 1)

        if (r is None) or (r.status_code != http.client.OK):
            msg = (
                f"Failed to retrieve '{url_for_cache_key}' "
                f"after {self._tries + 1} attempts. Skipping"
            )
            self._last_result_details["error"] = (msg, r or "timedout")

            if self.progress:
                print(msg)
            if r is not None:
                self.error_skips.append(r)
            else:
                # timeout
                self.error_skips.append("No response, timedout")
            raise HTTPReqError(http_response=r, msg=msg, url=url)

        if self._cache is not None and not skip_cache:
            self._cache.set(url_for_cache_key, r.text)

        self._last_result_details["retrieved_from"] = "web"

        if self.verbose:
            print()

        result = r.json() if parse_json else r.content
        return result
