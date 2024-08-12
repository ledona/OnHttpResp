"""test that httpreq is making expected calls to request"""

import http
import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from onhttpreq import ON_RESPONSE_RETURN_WAIT, ON_RESPONSE_WAIT_RETRY, HTTPReq, HTTPReqError


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
def test_get_w_cache_w_expiration(mock_requests: MagicMock):
    """test that get will work and that subsequent get will come from the the cache"""

    ref_json_result = {"data": 32}
    requests_get_return_value = _create_mock_request_get(
        text=json.dumps(ref_json_result), _json=ref_json_result
    )
    mock_requests.get.return_value = requests_get_return_value
    requests_kwargs = {"a": 1, "b": 2}
    timeout = 5
    url = "http://test.com/api.json"

    # create HTTPReq with in memory cache
    http_req = HTTPReq(
        cache_in_memory=True,
        request_timeout=timeout,
        requests_kwargs=requests_kwargs,
    )

    # use get to make a request
    resp = http_req.get(url)

    # test that requests.get was called correctly
    mock_requests.get.assert_called_once_with(url=url, timeout=timeout, **requests_kwargs)
    assert resp == ref_json_result

    # see if the response was cached
    assert http_req._cache is not None
    test_cached_json = http_req._cache.get_json(url)
    assert test_cached_json == ref_json_result

    # repeat the request
    mock_requests.get.reset_mock()
    resp = http_req.get(url)

    # see if the result came from the cache
    mock_requests.get.assert_not_called()
    assert resp == ref_json_result

    # expire it
    expiration_dt = datetime(2018, 2, 3, 19, 27)
    http_req.set_cached_expiration(url, expire_on_dt=expiration_dt)

    # call get again and test
    mock_requests.get.reset_mock()
    ref_new_json_result = {"data": 33}
    mock_requests.get.return_value = _create_mock_request_get(
        text=json.dumps(ref_new_json_result), _json=ref_new_json_result
    )

    # freeze time to prior to expiration and see if data still comes from cache
    with freeze_time(expiration_dt - timedelta(days=1)):
        mock_requests.get.assert_not_called()
        assert resp == ref_json_result

    # try again with current datetime
    resp = http_req.get(url)
    mock_requests.get.assert_called_once_with(url=url, timeout=timeout, **requests_kwargs)
    assert resp == ref_new_json_result


@patch("onhttpreq.http_req.requests")
def test_cache_ignore_expire(mock_requests):
    ref_json_result = {"data": 32}
    requests_get_return_value = _create_mock_request_get(
        text=json.dumps(ref_json_result), _json=ref_json_result
    )
    mock_requests.get.return_value = requests_get_return_value
    url = "http://test.com/api.json"

    # create HTTPReq with in memory cache
    http_req = HTTPReq(cache_in_memory=True, cache_dont_expire=True)

    # use get to make a request
    resp = http_req.get(url)

    # see if the response was cached
    assert http_req._cache is not None
    test_cached_json = http_req._cache.get_json(url)
    assert test_cached_json == ref_json_result

    # expire the data
    expiration_dt = datetime(2018, 2, 3, 19, 27)
    http_req.set_cached_expiration(url, expire_on_dt=expiration_dt)

    # repeat the request
    mock_requests.get.reset_mock()
    resp = http_req.get(url)

    # see if the result came from the cache
    mock_requests.get.assert_not_called()
    assert resp == ref_json_result


@patch("onhttpreq.http_req.requests")
def test_cache_overwrite(mock_requests):
    ref_first_json_result = {"data": "will be overwritten"}
    mock_requests.get.return_value = _create_mock_request_get(
        text=json.dumps(ref_first_json_result), _json=ref_first_json_result
    )
    url = "http://test.com/api.json"

    # create HTTPReq with in memory cache
    http_req = HTTPReq(cache_in_memory=True)

    # use get to make a request
    http_req.get(url)

    # see if the response was cached
    assert http_req._cache is not None
    test_cached_json = http_req._cache.get_json(url)
    assert test_cached_json == ref_first_json_result

    # set the cache to overwrite existing data
    ref_second_json_result = {"data": "overwritten"}
    mock_requests.get.reset_mock()
    mock_requests.get.return_value = _create_mock_request_get(
        text=json.dumps(ref_second_json_result), _json=ref_second_json_result
    )
    http_req.cache_overwrite = True

    # repeat the request
    resp = http_req.get(url)

    # see if the result came from the cache
    assert resp == ref_second_json_result
    test_cached_json = http_req._cache.get_json(url)
    assert test_cached_json == ref_second_json_result


@patch("onhttpreq.http_req.requests")
def test_on_response(mock_requests):
    # test that the on_response callback gets called

    mock_requests.get.return_value = _create_mock_request_get()
    on_response_mock = MagicMock()
    on_response_mock.return_value = None
    http_req = HTTPReq(on_response=on_response_mock)

    # use get to make a request
    url = "http://test.com/api.json"
    resp = http_req.get(url)

    # test that requests.get was called correctly
    mock_requests.get.assert_called_once_with(url=url, timeout=None)
    assert resp == mock_requests.get.return_value.json.return_value

    # test that the on_response method was called
    on_response_mock.assert_called_once_with(mock_requests.get.return_value)


@patch("onhttpreq.http_req.requests")
def test_retry(mock_requests):
    ref_json_result = {"data": "will eventually be returned"}
    req_get_fails = 0
    mock_error_resp = _create_mock_request_get(status_code=401)
    mock_success_resp = _create_mock_request_get(
        text=json.dumps(ref_json_result), _json=ref_json_result
    )

    def req_get_fails_5(*_, **__):
        # a request get function that will force a 5 retries
        nonlocal req_get_fails
        if req_get_fails < 5:
            req_get_fails += 1
            return mock_error_resp
        return mock_success_resp

    mock_requests.get.side_effect = req_get_fails_5

    # make a request that should fail
    http_req = HTTPReq(http_retries=4)
    url = "http://test.com/api.json"
    with pytest.raises(HTTPReqError) as excinfo:
        http_req.get(url)
    assert excinfo.value.http_resp == mock_error_resp
    assert mock_requests.get.call_count == 5

    # now something that should succeed
    http_req = HTTPReq(http_retries=6)
    resp = http_req.get(url)
    assert resp == mock_success_resp.json()
    assert mock_requests.get.call_count == 6


@patch("onhttpreq.http_req.requests")
@patch("onhttpreq.http_req.time.sleep")
def test_on_response_wait_retry(mock_sleep, mock_requests):
    ret = False
    duration = 60
    wait_kwargs = {"reason": "testing", "duration": duration}

    def on_response(resp):
        nonlocal ret
        if not ret:
            ret = True
            # respond with wait retry
            return ON_RESPONSE_WAIT_RETRY, wait_kwargs

    mock_requests.get.return_value = _create_mock_request_get()
    http_req = HTTPReq(on_response=on_response)
    url = "http://test.com/api.json"
    resp = http_req.get(url)

    assert resp == mock_requests.get.return_value.json.return_value
    mock_sleep.assert_called_once_with(duration)
    assert mock_requests.get.call_count == 2


@patch("onhttpreq.http_req.requests")
@patch("onhttpreq.http_req.time.sleep")
def test_on_response_return_wait(mock_sleep, mock_requests):
    ret = False
    duration = 60
    wait_kwargs = {"reason": "testing", "duration": duration}

    def on_response(resp):
        nonlocal ret
        if not ret:
            ret = True
            # respond with wait retry
            return ON_RESPONSE_RETURN_WAIT, wait_kwargs

    mock_requests.get.return_value = _create_mock_request_get()
    http_req = HTTPReq(on_response=on_response)
    url = "http://test.com/api.json"

    # freeze time so that there is no latency so sleep gets the full duration
    with freeze_time(datetime(2018, 2, 3, 20, 57)):
        resp = http_req.get(url)

        # it should not have waited
        mock_requests.get.assert_called_once_with(url=url, timeout=None)
        assert resp == mock_requests.get.return_value.json.return_value
        mock_sleep.assert_not_called()

        # should have waited this time
        mock_requests.get.reset_mock()
        resp = http_req.get(url)
        mock_requests.get.assert_called_once_with(url=url, timeout=None)
        assert resp == mock_requests.get.return_value.json.return_value
        mock_sleep.assert_called_once_with(duration)
