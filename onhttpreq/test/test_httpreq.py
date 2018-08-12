from freezegun import freeze_time
from unittest.mock import patch, MagicMock
import pytest
import unittest
from datetime import datetime, timedelta, timezone
import time
import json
import http

from ..http_req import (_HTTPCache, HTTPReq, HTTPReqError, _HTTPCacheJson,
                        ON_RESPONSE_WAIT_RETRY, ON_RESPONSE_RETURN_WAIT)


@pytest.mark.parametrize("store_as_compressed", [False, True])
def test_cache(store_as_compressed):
    cache = _HTTPCache(store_as_compressed=store_as_compressed)
    assert cache.get("url") is None
    assert cache.get_json("url") is None

    ref_json_text = b'["foo", {"bar":["baz", null, 1.0, 2]}]'
    ref_json = json.loads(ref_json_text)
    cache.set("url", ref_json_text)
    test_json_text = cache.get("url")

    assert ref_json_text == test_json_text
    test_json = cache.get_json("url")
    assert ref_json == test_json

    session = cache.sessionmaker()
    cache_result = session.query(_HTTPCacheJson) \
                          .filter(_HTTPCacheJson.url == "url") \
                          .one_or_none()
    assert (cache_result.json_bzip2 is not None) == store_as_compressed
    assert (cache_result.json is not None) != store_as_compressed
    session.close()


def test_expire():
    # behavior when expiration is disabled
    ts = time.time()
    utc_offset = 0
    _before_expiration = datetime(2017, 10, 22, 5, 53)
    _expire_on = datetime(2017, 10, 22, 5, 54)
    _after_expiration = datetime(2017, 10, 22, 5, 55)
    url = "url1"

    cache = _HTTPCache(dont_expire=True)
    cache.set(url, '[]', expire_on_dt=_expire_on)
    with freeze_time(_before_expiration):
        assert cache.get(url) is not None
    with freeze_time(_after_expiration):
        assert cache.get(url) is not None

    cache = _HTTPCache(dont_expire=False)
    cache.set(url, '[]', expire_on_dt=_expire_on)
    with freeze_time(_before_expiration):
        assert cache.get(url) is not None

    with freeze_time(_after_expiration):
        assert cache.get(url) is None

        ref_result = '["foo"]'
        cache.set(url, ref_result)
        assert ref_result == cache.get(url)


class TestHTTPReq(unittest.TestCase):
    """ test that httpreq is making expected calls to request """
    @staticmethod
    def _create_mock_request_get(status_code=http.client.OK, text=None, _json=None):
        """
        create a magic mock that can be uses as the return_value for
        requests.get
        """
        mock = MagicMock()
        mock.status_code = status_code
        if text is not None:
            mock.text = text
        if _json is not None:
            mock.json.return_value = _json

        return mock

    @patch("onhttpreq.http_req.requests")
    def test_get_w_cache_w_expiration(self, mock_requests):
        # test that get will work and that subsequent get will come from the the cache

        ref_json_result = {'data': 32}
        requests_get_return_value = self._create_mock_request_get(text=json.dumps(ref_json_result),
                                                                  _json=ref_json_result)
        mock_requests.get.return_value = requests_get_return_value
        requests_kwargs = {'a': 1, 'b': 2}
        timeout = 5
        url = "http://test.com/api.json"

        # create HTTPReq with in memory cache
        http_req = HTTPReq(cache_in_memory=True, request_timeout=timeout,
                           requests_kwargs=requests_kwargs)

        # use get to make a request
        resp = http_req.get(url)

        # test that requests.get was called correctly
        mock_requests.get.assert_called_once_with(url=url, timeout=timeout,
                                                  **requests_kwargs)
        self.assertEqual(ref_json_result, resp)

        # see if the response was cached
        test_cached_json = http_req._cache.get_json(url)
        self.assertEqual(ref_json_result, test_cached_json)

        # repeat the request
        mock_requests.get.reset_mock()
        resp = http_req.get(url)

        # see if the result came from the cache
        self.assertEqual(mock_requests.get.call_count, 0)
        self.assertEqual(ref_json_result, resp)

        # expire it
        expiration_dt = datetime(2018, 2, 3, 19, 27)
        http_req.set_cached_expiration(url, expiration_dt)

        # call get again and test
        mock_requests.get.reset_mock()
        ref_new_json_result = {'data': 33}
        mock_requests.get.return_value = self._create_mock_request_get(text=json.dumps(ref_new_json_result),
                                                                       _json=ref_new_json_result)

        # freeze time to prior to expiration and see if data still comes from cache
        with freeze_time(expiration_dt - timedelta(days=1)):
            self.assertEqual(mock_requests.get.call_count, 0)
            self.assertEqual(ref_json_result, resp)

        # try again with current datetime
        resp = http_req.get(url)
        mock_requests.get.assert_called_once_with(url=url, timeout=timeout, **requests_kwargs)
        self.assertEqual(ref_new_json_result, resp)

    @patch("onhttpreq.http_req.requests")
    def test_cache_ignore_expire(self, mock_requests):
        ref_json_result = {'data': 32}
        requests_get_return_value = self._create_mock_request_get(text=json.dumps(ref_json_result),
                                                                  _json=ref_json_result)
        mock_requests.get.return_value = requests_get_return_value
        url = "http://test.com/api.json"

        # create HTTPReq with in memory cache
        http_req = HTTPReq(cache_in_memory=True, cache_dont_expire=True)

        # use get to make a request
        resp = http_req.get(url)

        # see if the response was cached
        test_cached_json = http_req._cache.get_json(url)
        self.assertEqual(ref_json_result, test_cached_json)

        # expire the data
        expiration_dt = datetime(2018, 2, 3, 19, 27)
        http_req.set_cached_expiration(url, expiration_dt)

        # repeat the request
        mock_requests.get.reset_mock()
        resp = http_req.get(url)

        # see if the result came from the cache
        self.assertEqual(mock_requests.get.call_count, 0)
        self.assertEqual(ref_json_result, resp)

    @patch("onhttpreq.http_req.requests")
    def test_cache_overwrite(self, mock_requests):
        ref_first_json_result = {'data': 'will be overwritten'}
        mock_requests.get.return_value = self._create_mock_request_get(
            text=json.dumps(ref_first_json_result),
            _json=ref_first_json_result)
        url = "http://test.com/api.json"

        # create HTTPReq with in memory cache
        http_req = HTTPReq(cache_in_memory=True)

        # use get to make a request
        http_req.get(url)

        # see if the response was cached
        test_cached_json = http_req._cache.get_json(url)
        self.assertEqual(ref_first_json_result, test_cached_json)

        # set the cache to overwrite existing data
        ref_second_json_result = {'data': 'overwritten'}
        mock_requests.get.reset_mock()
        mock_requests.get.return_value = self._create_mock_request_get(
            text=json.dumps(ref_second_json_result),
            _json=ref_second_json_result)
        http_req.cache_overwrite = True

        # repeat the request
        resp = http_req.get(url)

        # see if the result came from the cache
        self.assertEqual(ref_second_json_result, resp)
        test_cached_json = http_req._cache.get_json(url)
        self.assertEqual(ref_second_json_result, test_cached_json)

    @patch("onhttpreq.http_req.requests")
    def test_on_response(self, mock_requests):
        # test that the on_response callback gets called

        mock_requests.get.return_value = self._create_mock_request_get()
        on_response_mock = MagicMock()
        on_response_mock.return_value = None
        http_req = HTTPReq(on_response=on_response_mock)

        # use get to make a request
        url = "http://test.com/api.json"
        resp = http_req.get(url)

        # test that requests.get was called correctly
        mock_requests.get.assert_called_once_with(url=url, timeout=None)
        self.assertEqual(mock_requests.get.return_value.json.return_value, resp)

        # test that the on_response method was called
        on_response_mock.assert_called_once_with(mock_requests.get.return_value)

    @patch("onhttpreq.http_req.requests")
    def test_retry(self, mock_requests):
        ref_json_result = {'data': 'will eventually be returned'}
        req_get_fails = 0
        mock_error_resp = self._create_mock_request_get(status_code=401)
        mock_success_resp = self._create_mock_request_get(text=json.dumps(ref_json_result),
                                                          _json=ref_json_result)

        def req_get_fails_5(*args, **kwargs):
            # a request get function that will force a 5 retries
            nonlocal req_get_fails
            if req_get_fails < 5:
                req_get_fails += 1
                return mock_error_resp
            else:
                return mock_success_resp

        mock_requests.get.side_effect = req_get_fails_5

        # make a request that should fail
        http_req = HTTPReq(http_retries=4)
        url = "http://test.com/api.json"
        with self.assertRaises(HTTPReqError) as cm:
            http_req.get(url)
        self.assertEqual(mock_error_resp, cm.exception.http_resp)
        self.assertEqual(mock_requests.get.call_count, 5)

        # now something that should succeed
        http_req = HTTPReq(http_retries=6)
        resp = http_req.get(url)
        self.assertEqual(mock_success_resp.json(), resp)
        self.assertEqual(mock_requests.get.call_count, 6)

    @patch("onhttpreq.http_req.requests")
    @patch("onhttpreq.http_req.time.sleep")
    def test_on_response_wait_retry(self, mock_sleep, mock_requests):
        ret = False
        duration = 60
        wait_kwargs = {'reason': "testing",
                       'duration': duration}

        def on_response(resp):
            nonlocal ret
            if not ret:
                ret = True
                # respond with wait retry
                return ON_RESPONSE_WAIT_RETRY, wait_kwargs

        mock_requests.get.return_value = self._create_mock_request_get()
        http_req = HTTPReq(on_response=on_response)
        url = "http://test.com/api.json"
        resp = http_req.get(url)

        self.assertEqual(mock_requests.get.return_value.json.return_value, resp)
        mock_sleep.assert_called_once_with(duration)
        self.assertEqual(2, mock_requests.get.call_count)

    @patch("onhttpreq.http_req.requests")
    @patch("onhttpreq.http_req.time.sleep")
    def test_on_response_return_wait(self, mock_sleep, mock_requests):
        ret = False
        duration = 60
        wait_kwargs = {'reason': "testing",
                       'duration': duration}

        def on_response(resp):
            nonlocal ret
            if not ret:
                ret = True
                # respond with wait retry
                return ON_RESPONSE_RETURN_WAIT, wait_kwargs

        mock_requests.get.return_value = self._create_mock_request_get()
        http_req = HTTPReq(on_response=on_response)
        url = "http://test.com/api.json"

        # freeze time so that there is no latency so sleep gets the full duration
        with freeze_time(datetime(2018, 2, 3, 20, 57)):
            resp = http_req.get(url)

            # it should not have waited
            mock_requests.get.assert_called_once_with(url=url, timeout=None)
            self.assertEqual(mock_requests.get.return_value.json.return_value, resp)
            self.assertEqual(0, mock_sleep.call_count)

            # should have waited this time
            mock_requests.get.reset_mock()
            resp = http_req.get(url)
            mock_requests.get.assert_called_once_with(url=url, timeout=None)
            self.assertEqual(mock_requests.get.return_value.json.return_value, resp)
            mock_sleep.assert_called_once_with(duration)
