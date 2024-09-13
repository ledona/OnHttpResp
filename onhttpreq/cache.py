import bz2
import json
import os
from datetime import UTC, datetime

from sqlalchemy import DateTime, Index, LargeBinary, String, create_engine, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, sessionmaker
from sqlalchemy.sql import text
from sqlalchemy.sql.expression import case

from .exception import OnHttpReqException


class _SQLAlchemyORMBase:  # pylint: disable=too-few-public-methods
    __allow_unmapped__ = True


_SQLAlchemyORMBase = declarative_base(cls=_SQLAlchemyORMBase)
"""Base class for all ORM objects"""

# TODO: convert to enum
# cache merge conflict modes
CONFLICT_MODE_FAIL = "fail"
CONFLICT_MODE_SKIP = "skip"
CONFLICT_MODE_OVERWRITE = "overwrite"


CURRENT_CACHE_DB_VERSION = 1


class HTTPCacheContent(_SQLAlchemyORMBase):
    __tablename__ = "content_cache"
    url: Mapped[str] = mapped_column(String(2000), primary_key=True)
    cached_on: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    content: Mapped[str] = mapped_column(String, nullable=True)
    content_bzip2 = mapped_column(LargeBinary, nullable=True)
    expire_on_dt: Mapped[datetime] = mapped_column(
        DateTime,
        doc="If current date/time is past this datetime then this "
        "record can be replaced by updated data",
        nullable=True,
    )


Index("ix_expire_on_dt", HTTPCacheContent.expire_on_dt)


def create_sessionmaker(filename, verbose=False):
    """returns: sessionmaker, engine"""
    db_path = ("/" + filename) if filename is not None else ""
    engine = create_engine("sqlite://" + db_path, echo=verbose)
    return sessionmaker(bind=engine), engine


class CacheOutOfDate(OnHttpReqException):
    pass


class CacheMergeConflict(OnHttpReqException):
    pass


class JSONParsingException(OnHttpReqException):
    """
    raised if there is an error parsing json to a dict

    json_text attribute will have the json that failed to parse
    """

    def __init__(self, msg, json_text: bytes):
        super().__init__(msg)
        self.json_text = json_text.decode("utf-8")


class CacheURLNotFound(OnHttpReqException):
    """raised if a url is not present in the cache during an operation that expects a
    url to be cached"""


class HTTPCache:
    """
    cache http responses to a DB
    """

    def __init__(
        self,
        filename=None,
        verbose=False,
        debug=False,
        dont_expire=False,
        store_as_compressed=False,
    ):
        """
        filename - if None then the DB will be in memory
        store_as_compressed - store in compressed form, and expect the cache to be compressed
        """
        create_cache = filename is None or not os.path.isfile(filename)
        self.dont_expire = dont_expire
        if create_cache and verbose:
            print(f"Creating cache file '{filename}'")

        self.sessionmaker, engine = create_sessionmaker(filename, verbose=debug)

        session = self.sessionmaker()
        try:
            if create_cache:
                _SQLAlchemyORMBase.metadata.create_all(engine)
                session.execute(text(f"pragma user_version = {CURRENT_CACHE_DB_VERSION}"))
                self.version = CURRENT_CACHE_DB_VERSION
            else:
                self.version = session.execute(text("pragma user_version")).fetchone()[0]
                ex_msg_prefix = (
                    f"Cache is out of date. Cache at '{filename}' has version "
                    f"'{self.version}'. Current version is '{CURRENT_CACHE_DB_VERSION}'."
                )

                if self.version != CURRENT_CACHE_DB_VERSION:
                    if self.version == 0:
                        # migrate to v1
                        migration_instructions = """alter table json_cache rename column json to content;
alter table json_cache rename column json_bzip2 to content_bzip2;
alter table json_cache rename to content_cache;
pragma user_version = 1;
"""
                        raise CacheOutOfDate(
                            f"{ex_msg_prefix} To migrate execute the following:\n{migration_instructions}"
                        )
                    raise CacheOutOfDate(ex_msg_prefix + " No instructions on how to migrate.")
        finally:
            session.close()
        self.store_as_compressed = store_as_compressed

    def get_info(self, url_glob=None, dt_range=None):
        """
        url_glob: glob pattern to filter urls
        return a dict with descriptive information for the cache"""
        result = {}
        filters = []
        if url_glob is not None:
            filters.append(HTTPCacheContent.url.op("GLOB")(url_glob))
        if dt_range is not None:
            if dt_range[0] is not None:
                filters.append(HTTPCacheContent.cached_on >= dt_range[0])
            if dt_range[1] is not None:
                filters.append(HTTPCacheContent.cached_on < dt_range[1])
        session = self.sessionmaker()
        try:
            result["n"] = session.query(HTTPCacheContent.url).filter(*filters).count()
            (
                result["earliest_dt"],
                result["latest_dt"],
                result["n_expirable"],
                result["n_not_compressed"],
                result["n_compressed"],
            ) = (
                session.query(
                    func.min(HTTPCacheContent.cached_on),
                    func.max(HTTPCacheContent.cached_on),
                    func.sum(case((HTTPCacheContent.expire_on_dt.isnot(None), 1), else_=0)),
                    func.sum(case((HTTPCacheContent.content.isnot(None), 1), else_=0)),
                    func.sum(case((HTTPCacheContent.content_bzip2.isnot(None), 1), else_=0)),
                )
                .filter(*filters)
                .one()
            )

            if result["n_not_compressed"] is None:
                result["n_not_compressed"] = 0
            if result["n_compressed"] is None:
                result["n_compressed"] = 0

        finally:
            session.close()

        return result

    def filter(self, url_glob=None, dt_range=None, dest_cache=None, delete=False):
        """
        filter for urls that match the regex. A url glob pattern or dt range is required

        dest_cache: if not None then update dest_cache to contain content that matches the filter
        delete: remove the urls from this cache
        dt_range: tuple of (start datetime, end datetime). Content will be filtered inclusive of the
           start datetime and exclusive of the end datetime. One datetime can be None indicating
           all content prior to end or after start

        returns: list of URLs that match the regex
        """
        if (url_glob is None) and (dt_range is None):
            raise ValueError("url_glob or dt_range must be not None")

        urls = []
        session = self.sessionmaker()
        dest_session = dest_cache.sessionmaker() if dest_cache is not None else None
        try:
            filters = []
            if url_glob is not None:
                filters.append(HTTPCacheContent.url.op("GLOB")(url_glob))
            if dt_range is not None:
                if dt_range[0] is not None:
                    filters.append(HTTPCacheContent.cached_on >= dt_range[0])
                if dt_range[1] is not None:
                    filters.append(HTTPCacheContent.cached_on < dt_range[1])

            for hcc in session.query(HTTPCacheContent).filter(*filters).all():
                urls.append(hcc.url)
                if delete:
                    session.delete(hcc)
                    session.flush()
                if dest_session is not None:
                    session.expunge(hcc)
                    dest_session.merge(hcc)

            if dest_session is not None:
                dest_session.commit()
            if delete:
                session.commit()
        finally:
            session.close()
            if dest_session is not None:
                dest_session.close()

        return urls

    def merge(self, cache_, conflict_mode=CONFLICT_MODE_FAIL):
        """
        merge another cache with the contents of this cache
        cache_: the cache to merge into this cache
        conflict_mode:
           CONFLICT_MODE_FAIL will raise CacheMergeConflict Exception if there is a conflict
           CONFLICT_MODE_SKIP will do nothing, skiping the merge
           CONFLICT_MODE_OVERWRITE will overwrite what's in the cache with the merge value
        returns: list or urls merged, list of conflict urls
        """
        if conflict_mode not in {
            CONFLICT_MODE_FAIL,
            CONFLICT_MODE_SKIP,
            CONFLICT_MODE_OVERWRITE,
        }:
            raise ValueError(f"Invalid conflict mode '{conflict_mode}'")

        session = self.sessionmaker()
        src_session = cache_.sessionmaker()
        urls = []
        conflict_urls = []
        try:
            for hcc in src_session.query(HTTPCacheContent).all():
                urls.append(hcc.url)
                existing_cache_entry = (
                    session.query(HTTPCacheContent.url)
                    .filter(HTTPCacheContent.url == hcc.url)
                    .one_or_none()
                )
                if existing_cache_entry is not None:
                    conflict_urls.append(hcc.url)
                    if conflict_mode == CONFLICT_MODE_FAIL:
                        raise CacheMergeConflict(f"URL '{hcc.url}' already exists")
                    elif conflict_mode == CONFLICT_MODE_SKIP:
                        # leave the original cache as is
                        continue

                # either this is a conflict and we are in overwrite mode or no conflict
                src_session.expunge(hcc)
                session.merge(hcc)
            session.commit()
        finally:
            session.close()
            src_session.close()

        return urls, conflict_urls

    def get(self, url):
        """return the content for url. returns None if the url is not in the cache"""
        session = self.sessionmaker()
        try:
            cache_result = (
                session.query(HTTPCacheContent).filter(HTTPCacheContent.url == url).one_or_none()
            )

            # if expiration is enabled then don't return anything that is expired
            if (
                cache_result is not None
                and not self.dont_expire
                and cache_result.expire_on_dt is not None
                and cache_result.expire_on_dt.replace(tzinfo=UTC) < datetime.now(UTC)
            ):
                print(
                    f"URL '{url}' found in cache, but set for expiration in the past at "
                    f"{cache_result.expire_on_dt}, so not returned."
                )
                cache_result = None
        finally:
            session.close()

        if cache_result is None:
            return None
        if cache_result.content is not None:
            return cache_result.content
        assert cache_result.content_bzip2 is not None
        return bz2.decompress(cache_result.content_bzip2)

    def get_json(self, url):
        content = self.get(url)
        if content is not None:
            try:
                json_result = json.loads(content)
                return json_result
            except json.JSONDecodeError as ex:
                raise JSONParsingException("Error parsing json", json_text=content) from ex
        else:
            return None

    def set(self, url, content, expire_on_dt=None, expire_time_delta=None, cached_on=None):
        """
        Use either expire_on_dt or expire_time_delta

        expire_on_dt - in UTC
        expire_time_delta - a timedelta object that will be added to datetime.now() to calculate the
           expire_on_dt
        """
        assert not (expire_on_dt is not None and expire_time_delta is not None)
        if expire_on_dt is None and expire_time_delta is not None:
            expire_on_dt = datetime.now(UTC) + expire_time_delta

        session = self.sessionmaker()
        try:
            if self.store_as_compressed:
                assert isinstance(content, (str, bytes))
                data = content if isinstance(content, bytes) else str.encode(content)
                kwarg_data = {"content_bzip2": bz2.compress(data)}
            else:
                kwarg_data = {"content": content}

            if cached_on is not None:
                kwarg_data["cached_on"] = cached_on

            cache_data = HTTPCacheContent(url=url, expire_on_dt=expire_on_dt, **kwarg_data)
            session.add(cache_data)
            try:
                session.commit()
            except IntegrityError as ie:
                # overwrite the existing value
                # this exception is sufficient for the sqlite3 cache implementation,
                # may not be reslient to updates
                if (
                    ie.args[0]
                    != "(sqlite3.IntegrityError) UNIQUE constraint failed: content_cache.url"
                ):
                    # there was some other exception
                    raise

                session.rollback()
                cache_data = (
                    session.query(HTTPCacheContent).filter(HTTPCacheContent.url == url).one()
                )

                if self.store_as_compressed:
                    data = content if isinstance(content, bytes) else str.encode(content)
                    cache_data.content_bzip2 = bz2.compress(data)
                else:
                    cache_data.content = content

                cache_data.expire_on_dt = expire_on_dt
                session.commit()
        finally:
            session.close()

    def get_expiration(self, url):
        """get the datetime that the URL is set to expire, raises exception if url is not in cache"""
        session = self.sessionmaker()
        try:
            _stat_cache = (
                session.query(HTTPCacheContent).filter(HTTPCacheContent.url == url).one_or_none()
            )
            if _stat_cache is None:
                raise CacheURLNotFound(url)
            return _stat_cache.expire_on_dt
        finally:
            session.close()

    def set_expiration(self, url, expire_on_dt=None, expire_time_delta=None):
        if expire_on_dt is None:
            assert expire_time_delta is not None
            expire_on_dt = datetime.now(UTC) + expire_time_delta
        elif expire_time_delta is not None:
            raise ValueError("Only one of expire_on_dt and expire_time_delta can be not None")

        session = self.sessionmaker()
        try:
            _stat_cache = (
                session.query(HTTPCacheContent).filter(HTTPCacheContent.url == url).one_or_none()
            )
            if _stat_cache is None:
                raise CacheURLNotFound(url)
            _stat_cache.expire_on_dt = expire_on_dt
            session.commit()
        finally:
            session.close()
