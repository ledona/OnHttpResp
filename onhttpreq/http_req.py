"""HTTP requester with caching for API requests"""

import http
import logging
import math
import time
import warnings
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Literal, TypedDict, cast

import curl_cffi.requests as curl_requests
import requests as basic_requests
import tqdm

from .cache import CacheIdentType, HTTPCache

_LOGGER = logging.getLogger(__name__)


class CacheOnlyError(Exception):
    """raised if cache_only is enabled and a url is not in the cache"""


class HTTPReqError(Exception):
    def __init__(self, http_response, msg="", url=None):
        """
        http_response: either a requests response or an exception
        """
        super().__init__(msg, http_response)
        self.http_resp = http_response
        self.msg = msg
        self.url = url

    def __str__(self):
        msg_prefix = f"HTTPReqError for url='{self.url}'"
        if isinstance(self.http_resp, Exception):
            return f"{msg_prefix} caused by exception {self.http_resp}"

        return f"""{msg_prefix}
msg '{self.msg}'
status code {self.http_resp.status_code}
Headers:
{self.http_resp.headers}
Content:
{self.http_resp.text}"""


# TODO: these should be an enum
ON_RESPONSE_WAIT_RETRY = "wait_retry"
"""wait for a specified duration then try again"""
ON_RESPONSE_RETURN_WAIT = "return_wait"
"""return the http response then wait for a specified duration"""
ON_RESPONSE_FAIL = "fail"
"""raise a failure for the http request"""

_LastResultDetailsCacheRetrievalKeyType = Literal["url", "cache_url", "cache_key"]
"""type for the cache_retrieval_key value"""


class _LastResultDetails(TypedDict, total=False):
    url: str
    """the http url for the requested content"""
    cache_url: str | None
    """the cachable url for the content"""
    cache_key: str | None
    """the cache key used for the content"""
    http_attempts: int
    retrieved_from: Literal["cache", "web"]
    """where was the content retrieved from"""
    cache_retrieval_key: _LastResultDetailsCacheRetrievalKeyType
    """if content was retrieved from the cache, which key was used"""
    error: tuple[str, Any]
    expire_on_dt: datetime | None


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
        curl_impersonate=False,
    ):
        """
        cache_in_memory: if true then create an in memory cache
        cache_only: results will only come from the cache. if a url is not available in the cache
          then an error occurs, when this is true nothing in the cache will be considered expired
        requests_kwargs: kwargs tp pass to requests when a get/request is made
        request_timeout: timeout in seconds for a request reqponse, if no response is received
          then a retry is attempted (if there are retries remaining)
        compression: compress the cache
        curl_impersonate: use curl impersonation
        on_response: A callback that can be used to process the http request responses prior to
          returning results. Useful for handling header data that should result in varying the
          behavior of the cache, handling rate limits, etc.
          This should be a function that takes a request response or an exception (in the case of no
          request failure), and returns None or
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
        self.curl_impersonate = curl_impersonate
        self.http_requests = 0

        self._last_result_details: _LastResultDetails = {}

        if verbose:
            _LOGGER.setLevel(logging.DEBUG)

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

    def _wait(
        self,
        reason: None | str = None,
        duration: None | int = None,
        started_waiting_dt: datetime | None = None,
    ):
        assert duration is not None

        self.total_wait_secs += duration
        if started_waiting_dt is None:
            started_waiting_dt = datetime.now(UTC)

        wait_till_dt = started_waiting_dt + timedelta(seconds=duration)
        wait_duration = (wait_till_dt - started_waiting_dt).total_seconds()
        if wait_duration <= 0:
            return

        _LOGGER.debug(
            "Rate limit reached, reason '%s'. Waiting %i seconds starting at %s",
            reason,
            duration,
            started_waiting_dt,
        )

        if self.progress:
            wait_iterator = tqdm.trange(
                math.ceil(wait_duration),
                desc=reason or "waiting on rate limit",
                leave=False,
            )
        else:
            wait_iterator = range(math.ceil(wait_duration))
        for _ in wait_iterator:
            time.sleep(1)
            if datetime.now(UTC) > wait_till_dt:
                # in case the computer hibernates, the progress will be off but
                # exit on time
                break

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
            self._return_wait_cmd = {"started_waiting_dt": datetime.now(UTC), **res[1]}
            return True

        if res[0] == ON_RESPONSE_FAIL:
            raise HTTPReqError(get_response, msg=res[1], url=url)

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
        cache_url: str | None = None,
        cache_key: str | None = None,
    ) -> _GetReturnType:
        """
        cache_fail_func: if cache is enabled and the url is not in the cache and this is not None
           then call this func. Useful for displaying messages
        cache_url: The url to store in the cache instead of the request URL. Useful if the\
            actual url contains information that should not be written to the cache.
        cache_key: if not None then the cache key will be set to this, if None then caching will\
            be based on the url or cache_url. required if cache_url is not None
        """
        assert not (skip_cache and self.cache_only), "This is an invalid combination"

        self._last_result_details = {
            "url": url,
            "cache_url": cache_url,
            "cache_key": cache_key,
            "http_attempts": 0,
        }

        if self._return_wait_cmd is not None:
            self._wait(**self._return_wait_cmd)
            self._return_wait_cmd = None
        self.requests += 1
        _LOGGER.debug("HTTP request: '%s' : %s", url, self._requests_kwargs)

        result = None
        if self._cache is not None and not self.cache_overwrite and not skip_cache:
            searches: list[tuple[str, CacheIdentType, _LastResultDetailsCacheRetrievalKeyType]] = [
                (url, "url", "url")
            ]
            if cache_url is not None:
                searches.append((cache_url, "url", "cache_url"))
            if cache_key is not None:
                searches.append((cache_key, "key", "cache_key"))
            for ident, ident_type, cache_retrieval_key_type in searches:
                result = (
                    self._cache.get_json(ident, ident_type=ident_type)
                    if parse_json
                    else self._cache.get(ident, ident_type=ident_type)
                )
                if result is not None:
                    self._last_result_details["retrieved_from"] = "cache"
                    self._last_result_details["expire_on_dt"] = self._cache.get_expiration(
                        ident, ident_type=ident_type
                    )
                    self._last_result_details["cache_retrieval_key"] = cache_retrieval_key_type
                    self.requests_from_cache += 1
                    return cast(HTTPReq._GetReturnType, result)

        if self.cache_only:
            raise CacheOnlyError(f"{url=}|{cache_key=} not in cache '{self.cache_filename}'")

        # cache search failed
        if cache_fail_func is not None:
            cache_fail_func()

        self._tries = 0

        requests = curl_requests if self.curl_impersonate else basic_requests

        while self._tries < self._retries + 1:
            self._tries += 1
            self.http_requests += 1
            try:
                self._last_result_details["http_attempts"] += 1
                r = requests.get(url=url, timeout=self._request_timeout, **self._requests_kwargs)
            except requests.exceptions.Timeout as ex:
                r = ex
                _LOGGER.error("HTTPReq request timed out... : %s", ex)
            except requests.exceptions.ConnectionError as ex:
                r = ex
                _LOGGER.error("HTTPReq request failed to connect... : %s", ex)

            # if not isinstance(r, Exception) and _LOGGER.getEffectiveLevel == logging.DEBUG:
            #     print(
            #         f"HTTPReq response for attempt {self._tries + 1}/{self._retries} "
            #         f"code: {r.status_code}"
            #     )
            #     print(f"HTTPReq Headers: {r.headers}")
            #     print()
            #     print(r.text)

            if self._on_response is not None:
                if self._process_on_response(r, url):
                    break
            elif r is not None and not isinstance(r, Exception) and r.status_code == http.client.OK:
                break

        self.total_retries += max(0, self._tries - 1)

        if (r is None) or isinstance(r, Exception) or (r.status_code != http.client.OK):
            msg = f"Failed to retrieve '{url}' " f"after {self._tries + 1} attempts. Skipping"
            self._last_result_details["error"] = (msg, r or "timedout")

            if self.progress:
                _LOGGER.info(msg)
            if r is not None:
                self.error_skips.append(r)
            else:
                # timeout
                self.error_skips.append("No response, timedout")
            raise HTTPReqError(r, msg=msg, url=url)

        if self._cache is not None and not skip_cache:
            # save to cache
            self._cache.set(cache_url or url, r.text, cache_key=cache_key)

        self._last_result_details["retrieved_from"] = "web"

        result = r.json() if parse_json else r.content
        return result
