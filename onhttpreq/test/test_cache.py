from freezegun import freeze_time
import pytest
import json
from datetime import datetime
import time

from .. import HTTPReq, HTTPReqError
from ..cache import (HTTPCache, HTTPCacheContent, CacheMergeConflict,
                     CONFLICT_MODE_OVERWRITE, CONFLICT_MODE_SKIP, CONFLICT_MODE_FAIL)


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
    cache.set('url1', "content A", cached_on=REF_EARLY_DT)
    cache.set('url2', "content B", cached_on=REF_EARLY_DT)
    cache.set('url3', "content C", expire_on_dt=datetime.now(), cached_on=REF_LAST_DT)


# reference information that applies to compressed and uncompressed test caches
BASE_REF_INFO = {
    'n' : 3,
    'earlier_dt': REF_EARLY_DT,
    'latest_dt': REF_LAST_DT,
    'n_expirable': 1,
    'n_compressed': 0,
    'n_not_compressed': 0
}


@pytest.mark.parametrize("compressed", [True, False])
def test_info(compressed):
    cache = HTTPCache(store_as_compressed=compressed)
    _populate_fake_cache(cache)

    info = cache.get_info()
    ref_info = dict(BASE_REF_INFO)
    ref_info['n_compressed'] = ref_info['n'] if compressed else 0
    ref_info['n_not_compressed'] = ref_info['n'] if not compressed else 0
    assert ref_info == info

@pytest.fixture
def compressed_cache():
    cache = HTTPCache(store_as_compressed=True)
    _populate_fake_cache(cache)
    return cache


def test_info_w_regex(compressed_cache):
    info = compressed_cache.get_info(url_pattern="url[12]")
    ref_info = dict(BASE_REF_INFO)
    ref_info.update({
        'n': 2,
        'latest_dt': REF_EARLY_DT,
        'n_compressed': 2,
        'n_expirable': 0
    })
    assert ref_info == info


def test_filter(compressed_cache):
    urls = compressed_cache.filter("url[12]")
    assert {'url1', 'url2'} == set(urls)


@pytest.mark.parametrize("delete, dest",
                         [(True, True),
                          (True, False),
                          (False, True)])
def test_filter_w_dest(compressed_cache, delete, dest):
    dest_cache = HTTPCache(store_as_compressed=True) if dest else None
    urls = compressed_cache.filter("url[12]", dest_cache=dest_cache, delete=delete)
    assert {'url1', 'url2'} == set(urls)

    if dest:
        urls = dest_cache.filter("url[12]")
        assert {'url1', 'url2'} == set(urls)
        info = dest_cache.get_info()
        ref_info = dict(BASE_REF_INFO)
        ref_info.update({
            'n': 2,
            'latest_dt': REF_EARLY_DT,
            'n_compressed': 2,
            'n_expirable': 0
        })
        assert ref_info == info

    info = compressed_cache.get_info()
    urls = compressed_cache.filter("url[12]")
    if delete:
        assert info['n'] == 1
        assert len(urls) == 0
    else:
        assert info['n'] == 3
        assert {'url1', 'url2'} == set(urls)


def test_merge(compressed_cache):
    cache_ = HTTPCache(store_as_compressed=True)
    cache_.set('url4', "content D", cached_on=REF_LAST_DT)

    compressed_cache.merge(cache_)

    info = compressed_cache.get_info()
    ref_info = dict(BASE_REF_INFO)
    ref_info['n'] += 1
    ref_info['n_compressed'] = ref_info['n']
    assert ref_info == info

    urls = compressed_cache.filter("url4")
    assert ['url4'] == urls


@pytest.fixture(scope='module')
def merge_cache():
    cache_ = HTTPCache(store_as_compressed=True)
    cache_.set('url0', "content Z", cached_on=REF_EARLY_DT)
    cache_.set('url2', "content X", cached_on=REF_LAST_DT)
    cache_.set('url3', "content Y", cached_on=REF_LAST_DT)
    cache_.set('url4', "content D", cached_on=REF_LAST_DT)
    return cache_


@pytest.mark.parametrize("conflict_mode", (CONFLICT_MODE_OVERWRITE, CONFLICT_MODE_SKIP, CONFLICT_MODE_FAIL))
def test_merge_w_conflict(merge_cache, compressed_cache, conflict_mode):
    compressed_cache.dont_expire = True
    test_exception = None
    try:
        merged_urls, conflict_urls = compressed_cache.merge(merge_cache, conflict_mode=conflict_mode)
    except CacheMergeConflict as ex:
        test_exception = ex
        pass

    ref_data = {'url1': 'content A'}

    if conflict_mode != CONFLICT_MODE_FAIL:
        assert {'url0', 'url2', 'url3', 'url4'} == set(merged_urls)
        assert {'url2', 'url3'} == set(conflict_urls)

        ref_data.update({
            'url0': 'content Z',
            'url4': 'content D'
        })
        if conflict_mode == CONFLICT_MODE_OVERWRITE:
            ref_data.update({
                'url2': 'content X',
                'url3': 'content Y'
            })
        elif conflict_mode == CONFLICT_MODE_SKIP:
            ref_data.update({
                'url2': 'content B',
                'url3': 'content C'
            })
        else:
            raise ValueError("Don't know how to test for conflict_mode '{}'".format(conflict_mode))
    else:
        assert test_exception is not None
        ref_data.update({
            'url2': 'content B',
            'url3': 'content C'
        })

    test_data = {
        url: compressed_cache.get(url).decode()
        for url in compressed_cache.filter("*")
    }
    assert ref_data == test_data
