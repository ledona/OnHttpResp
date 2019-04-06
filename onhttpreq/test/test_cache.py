from freezegun import freeze_time
import pytest
import json
from datetime import datetime
import time

from .. import HTTPReq, HTTPReqError
from ..cache import HTTPCache, HTTPCacheContent


@pytest.mark.parametrize("store_as_compressed", [False, True])
def test_cache(store_as_compressed):
    cache = HTTPCache(store_as_compressed=store_as_compressed)
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
    cache_result = session.query(HTTPCacheContent) \
                          .filter(HTTPCacheContent.url == "url") \
                          .one_or_none()
    assert (cache_result.content_bzip2 is not None) == store_as_compressed
    assert (cache_result.content is not None) != store_as_compressed
    session.close()


def test_expire():
    # behavior when expiration is disabled
    ts = time.time()
    utc_offset = 0
    _before_expiration = datetime(2017, 10, 22, 5, 53)
    _expire_on = datetime(2017, 10, 22, 5, 54)
    _after_expiration = datetime(2017, 10, 22, 5, 55)
    url = "url1"

    cache = HTTPCache(dont_expire=True)
    cache.set(url, '[]', expire_on_dt=_expire_on)
    with freeze_time(_before_expiration):
        assert cache.get(url) is not None
    with freeze_time(_after_expiration):
        assert cache.get(url) is not None

    cache = HTTPCache(dont_expire=False)
    cache.set(url, '[]', expire_on_dt=_expire_on)
    with freeze_time(_before_expiration):
        assert cache.get(url) is not None

    with freeze_time(_after_expiration):
        assert cache.get(url) is None

        ref_result = '["foo"]'
        cache.set(url, ref_result)
        assert ref_result == cache.get(url)


def test_info():
    raise NotImplementedError()


def test_filter():
    raise NotImplementedError()


def test_merge():
    raise NotImplementedError()
