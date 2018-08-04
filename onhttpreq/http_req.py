"""
Shared HTTP requester for API requests
"""
from datetime import datetime, timedelta
import time
import http
import tqdm
import requests
from sqlalchemy.schema import Index
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
import os
import json
import math
from sqlalchemy.orm import sessionmaker


_SQLAlchemyORMBase = declarative_base()


class _HTTPCacheJson(_SQLAlchemyORMBase):
    __tablename__ = 'json_cache'
    url = sqlalchemy.Column(sqlalchemy.String(2000), primary_key=True)
    cached_on = sqlalchemy.Column(sqlalchemy.DateTime, default=sqlalchemy.func.now())
    json = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    expire_on_dt = sqlalchemy.Column(sqlalchemy.DateTime,
                                     doc="If current date/time is past this datetime then this record can be deleted")


Index('ix_expire_on_dt', _HTTPCacheJson.expire_on_dt)


class _HTTPCache(object):
    """
    cache http responses to a DB
    """
    def __init__(self, filename=None, verbose=False, dont_expire=False):
        """
        filename - if None then the DB will be in memory
        """
        create_cache = filename is None or not os.path.isfile(filename)
        self._dont_expire = dont_expire
        if create_cache and verbose:
            print("Creating cache file '{}'".format(filename))

        db_path = ('/' + filename) if filename is not None else ""
        self.engine = sqlalchemy.create_engine('sqlite://' + db_path, echo=verbose)
        self.sessionmaker = sessionmaker(bind=self.engine)

        if create_cache:
            _SQLAlchemyORMBase.metadata.create_all(self.engine)

    def get(self, url):
        session = self.sessionmaker()
        cache_result = session.query(_HTTPCacheJson) \
                              .filter(_HTTPCacheJson.url == url) \
                              .one_or_none()

        # if expiration is enabled then don't return anything that is expired
        if cache_result is not None and \
           not self._dont_expire and \
           cache_result.expire_on_dt is not None and \
           cache_result.expire_on_dt < datetime.utcnow():
            cache_result = None

        session.close()
        return cache_result.json if cache_result is not None else None

    def get_json(self, url):
        text = self.get(url)
        if text is not None:
            json_result = json.loads(text)
            return json_result
        else:
            return None

    def set(self, url, json_text, expire_on_dt=None):
        """
        expire_on_dt - in UTC
        """
        session = self.sessionmaker()
        cache_data = _HTTPCacheJson(url=url, json=json_text, expire_on_dt=expire_on_dt)
        session.add(cache_data)
        try:
            session.commit()
        except sqlalchemy.exc.IntegrityError as e:
            # this exception is sufficient for the sqlite3 cache implementation, may not be reslient to updates
            if e.args[0] != "(sqlite3.IntegrityError) UNIQUE constraint failed: json_cache.url":
                raise

            session.rollback()
            cache_data = session.query(_HTTPCacheJson) \
                                .filter(_HTTPCacheJson.url == url) \
                                .one()
            cache_data.json = json_text
            cache_data.expire_on_dt = expire_on_dt
            session.commit()
        session.close()

    def set_expiration(self, url, expire_on_dt):
        session = self.sessionmaker()
        _stat_cache = session.query(_HTTPCacheJson) \
                             .filter(_HTTPCacheJson.url == url) \
                             .one()
        _stat_cache.expire_on_dt = expire_on_dt
        session.commit()
        session.close()


class HTTPReqError(Exception):
    def __init__(self, http_response=None, msg=None):
        super().__init__(msg, http_response)
        self.http_resp = http_response
        self.msg = msg

    def __str__(self):
        return "HTTPReqError msg '{}'\nstatus code {}\nHeaders:\n{}\nContent:\n{}" \
            .format(self.msg,
                    self.http_resp.status_code if self.http_resp is not None else None,
                    self.http_resp.headers if self.http_resp is not None else None,
                    self.http_resp.text if self.http_resp is not None else None)


ON_RESPONSE_WAIT_RETRY = "wait_retry"
ON_RESPONSE_RETURN_WAIT = "return_wait"


class HTTPReq(object):
    def __init__(self, verbose=False, progress=False,
                 http_retries=2, requests_kwargs=None,
                 on_response=None, request_timeout=None,
                 cache_filename=None, cache_in_memory=False, cache_overwrite=False,
                 cache_dont_expire=False):
        """
        cache_in_memory - if true then create an in memory cache
        requests_kwargs - kwargs tp pass to requests when a get/request is made
        request_timeout - timeout in seconds for a request reqponse, if no response is received
          then a retry is attempted (if there are retries remaining)
        on_response - A callback that can be used to process the http request responses prior to
          returning results. Useful for handling header data that should result in varying the
          behavior of the cache, handling rate limits, etc.
          This should be a function a request response, and returns None or
          a tuple command in the form of a tuple (cmd, args_dict). If None is returned then no additional
          processing will be executed and the request response will be returned to the caller
          Available cmds and the arg_dict keys are

          ON_RESPONSE_WAIT_RETRY: 'reason', 'duration'  - Wait for duration seconds then repeat the request
            if this is used with progress then duration will be rounded up to the nearest second
          ON_RESPONSE_RETURN_WAIT: 'duration' - return the response to the caller but do not execute any new
             requests until the duration has expired
        """
        assert not ((cache_filename is not None) and cache_in_memory), \
            "caching can't both be in memory and to a file"

        self.cache_overwrite = cache_overwrite
        self.cache_filename = cache_filename
        self._cache = (_HTTPCache(filename=cache_filename, verbose=verbose, dont_expire=cache_dont_expire)
                       if (cache_filename is not None) or (cache_in_memory is True)
                       else None)

        self._requests_kwargs = requests_kwargs or {}
        self._request_timeout = request_timeout
        self._retries = http_retries
        self.requests = 0
        self.requests_from_cache = 0
        self._return_wait_cmd = None
        self.error_skips = []
        self.total_wait_secs = 0
        self.total_retries = 0
        self._on_response = on_response
        self.progress = progress
        self.verbose = verbose
        self.http_requests = 0

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
                for _ in tqdm.trange(math.ceil(duration), desc="waiting on rate limit", leave=False):
                    time.sleep(1)

            else:
                msg = "Rate limit reached, reason '{}'. Waiting {} seconds starting at {:%X}" \
                      .format(reason, duration, started_waiting_dt)
                print("\n" + msg)
                # test for positive duration just in case testing or other processing causes latency
                if duration > 0:
                    time.sleep(duration)
        elif duration > 0:
            time.sleep(duration)

    def _process_on_response(self, get_response):
        """
        returns - true if the retry loop should be broken
        raises - ValueError if the on_response method returned an invalid result
        """
        res = self._on_response(get_response)
        if res is None:
            return True
        else:
            if res[0] == ON_RESPONSE_WAIT_RETRY:
                if self.__tries < self._retries + 1:
                    # only makes sense to wait if there is another retry available
                    self._wait(**res[1])
            elif res[0] == ON_RESPONSE_RETURN_WAIT:
                self._return_wait_cmd = dict(res[1])
                self._return_wait_cmd['started_waiting_dt'] = datetime.now()
                return True
            else:
                raise ValueError("on_response returned an unknown command. {}".format(res))
        return False

    def set_cached_expiration(self, url, dt):
        """
        dt - this should be in UTC
        """
        assert self._cache is not None, "Caching is not enabled!"
        self._cache.set_expiration(url, dt)

    @property
    def history(self):
        """ return a dict describing the request history """
        return {'requests': self.requests,
                'requests_from_http': self.http_requests,
                'requests_from_cache': self.requests_from_cache,
                'wait_secs': self.total_wait_secs,
                'error_skips': self.error_skips,
                'request_retries': self.total_retries}

    def get(self, url, parse_json=True):
        if self._return_wait_cmd is not None:
            self._wait(**self._return_wait_cmd)
            self._return_wait_cmd = None
        self.requests += 1
        if self.verbose:
            print("\nHTTP request: '{url}' : '{kwargs}'\n".format(url=url,
                                                                  kwargs=self._requests_kwargs))

        result = None
        if self._cache is not None and not self.cache_overwrite:
            result = (self._cache.get_json(url)
                      if parse_json else
                      self._cache.get(url))
            if result is not None:
                self.requests_from_cache += 1
            if self.verbose:
                print("result {}found in cache".format("not " if result is not None else ""))

        if result is None:
            # cache search failed
            self.__tries = 0
            while self.__tries < self._retries + 1:
                self.__tries += 1
                self.http_requests += 1
                try:
                    r = requests.get(url=url, timeout=self._request_timeout, **self._requests_kwargs)
                except requests.exceptions.Timeout as ex:
                    r = None
                    if self.verbose:
                        print("HTTPReq request timed out... {}".format(ex))

                if self.verbose and r is not None:
                    print("HTTPReq response for attempt {}/{} code: {}".format(self.__tries + 1,
                                                                               self._retries,
                                                                               r.status_code))
                    print("HTTPReq Headers: {}".format(r.headers))
                    print()
                    print(r.text)

                if self._on_response is not None:
                    if self._process_on_response(r):
                        break
                elif r is not None and r.status_code == http.client.OK:
                    break

                if self.verbose:
                    print("Retry #{}".format(self.__tries + 1))

            self.total_retries += max(0, self.__tries - 1)

            if r is None or r.status_code != http.client.OK:
                msg = "Failed to retrieve '{}' after {} attempts. Skipping" \
                      .format(url, self.__tries + 1)
                if self.progress:
                    print(msg)
                if r is not None:
                    self.error_skips.append(r)
                else:
                    # timeout
                    self.error_skips.append("No response, timedout")
                raise HTTPReqError(http_response=r, msg=msg)

            if self.verbose:
                print()
            if self._cache is not None:
                self._cache.set(url, r.text)
            result = r.json() if parse_json else r.content

        return result