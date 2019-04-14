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


REF_EARLY_DT = datetime(2019, 4, 6, 18, 50)
REF_LAST_DT = datetime(2019, 4, 6, 18, 52)


def _populate_fake_cache(cache):
    cache.set('url1', "content", cached_on=REF_EARLY_DT)
    cache.set('url2', "content", cached_on=REF_EARLY_DT)
    cache.set('url3', "content", expire_on_dt=datetime.now(), cached_on=REF_LAST_DT)


# reference information that applies to compressed and uncompressed test caches
BASE_REF_INFO = {
    'n' : 3,
    'earliest_dt': REF_EARLY_DT,
    'latest_dt': REF_LAST_DT,
    'n_expirable': 1
}


@pytest.mark.parametrize("compressed", [True, False])
def test_compressed(compressed):
    cache = HTTPCache(store_as_compressed=compressed)
    _populate_fake_cache(cache)

    info = cache.info
    ref_info = dict(BASE_REF_INFO)
    ref_info['n_compressed'] = ref_info['n'] if compressed else 0
    ref_info['n_not_compressed'] = ref_info['n'] if not compressed else 0
    assert ref_info == info


def test_filter():
    raise NotImplementedError()


def test_merge():
    raise NotImplementedError()
