"""
compress or decompress a cache
"""
import argparse
from tqdm import tqdm
import bz2
from sqlalchemy import sql

from .http_req import _HTTPCacheJson, create_sessionmaker


def compress_func(row, preserve):
    if (row.json is None) or (row.json_bzip2 is not None):
        raise ValueError("Nothing to do for url: " + row.url)
    row.json_bzip2 = bz2.compress(str.encode(row.json))
    if not preserve:
        row.json = None


def decompress_func(row, preserve):
    if (row.json is not None) or (row.json_bzip2 is None):
        raise ValueError("Nothing to do for url: " + row.url)
    row.json = bz2.decompress(row.json_bzip2)
    if not preserve:
        row.json_bzip2 = None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compress/Decompress http cache in place")
    parser.add_argument("--decompress", help="Decomression mode. Default is to compress cache",
                        action="store_true", default=False)
    parser.add_argument("--skip_nodata", action="store_true", default=False,
                        help=("Only look at rows that will be (un)compressed. "
                              "By default an error will occur if (un)compress "
                              "cannot be performed on a line"))
    parser.add_argument("--preserve", action="store_true", default=False,
                        help=("Don't delete any existing data. By default the original "
                              "(un)compressed data will be removed as the "
                              "newly (un)compressed data is added."))

    parser.add_argument("filename")
    args = parser.parse_args()

    # open the dbs
    session = create_sessionmaker(args.filename)()

    query = session.query(_HTTPCacheJson)
    rows = query.count()

    if args.decompress:
        func = decompress_func
        if args.skip_nodata:
            query = query.filter(_HTTPCacheJson.json != sql.expression.null(),
                                 _HTTPCacheJson.json_bzip2 == sql.expression.null())
    else:
        func = compress_func
        if args.skip_nodata:
            query = query.filter(_HTTPCacheJson.json == sql.expression.null(),
                                 _HTTPCacheJson.json_bzip2 != sql.expression.null())

    for cache_row in tqdm(query.all(),
                          total=rows,
                          desc="{} cache rows".format(
                              "Decompressing" if args.decompress else "Compressing")):
        func(cache_row, args.preserve)
        session.commit()

    session.execute("vacuum")
    session.close()
