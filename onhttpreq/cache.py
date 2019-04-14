import bz2
import json
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Index
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import func
import os
from datetime import datetime
from sqlalchemy.sql.expression import case

_SQLAlchemyORMBase = declarative_base()


CURRENT_CACHE_DB_VERSION = 1


class HTTPCacheContent(_SQLAlchemyORMBase):
    __tablename__ = 'content_cache'
    url = sqlalchemy.Column(sqlalchemy.String(2000), primary_key=True)
    cached_on = sqlalchemy.Column(sqlalchemy.DateTime, default=sqlalchemy.func.now())
    content = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    content_bzip2 = sqlalchemy.Column(sqlalchemy.LargeBinary, nullable=True)
    expire_on_dt = sqlalchemy.Column(
        sqlalchemy.DateTime,
        doc="If current date/time is past this datetime then this record can be replaced by updated data")


Index('ix_expire_on_dt', HTTPCacheContent.expire_on_dt)


def create_sessionmaker(filename, verbose=False):
    """ returns: sessionmaker, engine """
    db_path = ('/' + filename) if filename is not None else ""
    engine = sqlalchemy.create_engine('sqlite://' + db_path, echo=verbose)
    return sessionmaker(bind=engine), engine


class CacheOutOfDate(Exception):
    pass


class HTTPCache(object):
    """
    cache http responses to a DB
    """
    def __init__(self, filename=None, verbose=False, dont_expire=False, store_as_compressed=False):
        """
        filename - if None then the DB will be in memory
        store_as_compressed - store in compressed form, and expect the cache to be compressed
        """
        create_cache = filename is None or not os.path.isfile(filename)
        self._dont_expire = dont_expire
        if create_cache and verbose:
            print("Creating cache file '{}'".format(filename))

        self.sessionmaker, engine = create_sessionmaker(filename, verbose=verbose)

        if create_cache:
            _SQLAlchemyORMBase.metadata.create_all(engine)
            engine.execute("pragma user_version = {}".format(CURRENT_CACHE_DB_VERSION))
            self.version = CURRENT_CACHE_DB_VERSION
        else:
            self.version = engine.execute("pragma user_version").fetchone()[0]
            ex_msg_prefix = "Cache is out of date. Cache at '{}' has version '{}'. Current version is '{}'.".format(
                filename, self.version, CURRENT_CACHE_DB_VERSION)

            if self.version != CURRENT_CACHE_DB_VERSION:
                if self.version == 0:
                    # migrate to v1
                    migration_instructions = """alter table json_cache rename column json to content;
alter table json_cache rename column json_bzip2 to content_bzip2;
alter table json_cache rename to content_cache;
pragma user_version = 1;
"""
                    raise CacheOutOfDate(ex_msg_prefix + " To migrate execute the following:\n{}"
                                         .format(migration_instructions))
                else:
                    raise CacheOutOfDate(ex_msg_prefix + " No instructions on how to migrate.")

        self.store_as_compressed = store_as_compressed

    @property
    def info(self):
        """ return a dict with descriptive information for the cache """
        result = {}
        session = self.sessionmaker()
        try:
            result['n'] = session.query(HTTPCacheContent.url).count()
            (result['earlier_dt'], result['latest_dt'], result['n_expirable'],
             result['n_not_compressed'], result['n_compressed']) = \
                session.query(func.min(HTTPCacheContent.cached_on),
                              func.max(HTTPCacheContent.cached_on),
                              func.sum(case([(HTTPCacheContent.expire_on_dt.isnot(None), 1)], 0)),
                              func.sum(case([(HTTPCacheContent.content.isnot(None), 1)], 0)),
                              func.sum(case([(HTTPCacheContent.content_bzip2.isnot(None), 1)], 0))) \
                       .one()

            if result['n_not_compressed'] is None:
                result['n_not_compressed'] = 0
            if result['n_compressed'] is None:
                result['n_compressed'] = 0

        finally:
            session.close()

        return result

    def filter(self, url_filter, filepath=None, delete_after_export=False):
        raise NotImplementedError()

    def merge(self, filepath, new_filepath=None):
        raise NotImplementedError()

    def get(self, url):
        session = self.sessionmaker()
        try:
            cache_result = session.query(HTTPCacheContent) \
                                  .filter(HTTPCacheContent.url == url) \
                                  .one_or_none()

            # if expiration is enabled then don't return anything that is expired
            if cache_result is not None and \
               not self._dont_expire and \
               cache_result.expire_on_dt is not None and \
               cache_result.expire_on_dt < datetime.utcnow():
                cache_result = None
        finally:
            session.close()

        if cache_result is None:
            return None
        elif cache_result.content is not None:
            return cache_result.content
        else:
            assert cache_result.content_bzip2 is not None
            return bz2.decompress(cache_result.content_bzip2)

    def get_json(self, url):
        text = self.get(url)
        if text is not None:
            json_result = json.loads(text)
            return json_result
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
            expire_on_dt = datetime.utcnow() + expire_time_delta

        session = self.sessionmaker()
        try:
            if self.store_as_compressed:
                assert isinstance(content, (str, bytes))
                data = content if isinstance(content, bytes) else str.encode(content)
                kwarg_data = {'content_bzip2': bz2.compress(data)}
            else:
                kwarg_data = {'content': content}

            if cached_on is not None:
                kwarg_data['cached_on'] = cached_on

            cache_data = HTTPCacheContent(url=url, expire_on_dt=expire_on_dt, **kwarg_data)
            session.add(cache_data)
            try:
                session.commit()
            except sqlalchemy.exc.IntegrityError as e:
                # overwrite the existing value

                # this exception is sufficient for the sqlite3 cache implementation, may not be reslient to updates
                if e.args[0] != "(sqlite3.IntegrityError) UNIQUE constraint failed: content_cache.url":
                    # there was some other exception
                    raise

                session.rollback()
                cache_data = session.query(HTTPCacheContent) \
                                    .filter(HTTPCacheContent.url == url) \
                                    .one()

                if self.store_as_compressed:
                    data = content if isinstance(content, bytes) else str.encode(content)
                    cache_data.content_bzip2 = bz2.compress(data)
                else:
                    cache_data.content = content

                cache_data.expire_on_dt = expire_on_dt
                session.commit()
        finally:
            session.close()

    def set_expiration(self, url, expire_on_dt=None, expire_time_delta=None):
        if expire_on_dt is None:
            assert expire_time_delta is not None
            expire_on_dt = datetime.utcnow() + expire_time_delta
        elif expire_time_delta is not None:
            raise ValueError("Only one of expire_on_dt and expire_time_delta can be not None")

        session = self.sessionmaker()
        try:
            _stat_cache = session.query(HTTPCacheContent) \
                                 .filter(HTTPCacheContent.url == url) \
                                 .one()
            _stat_cache.expire_on_dt = expire_on_dt
            session.commit()
        finally:
            session.close()
