#!/usr/bin/env python
import argparse
import os
from pprint import pprint

from dateutil import parser as du_parser

from onhttpreq.cache import (
    CONFLICT_MODE_FAIL,
    CONFLICT_MODE_OVERWRITE,
    CONFLICT_MODE_SKIP,
    HTTPCache,
)


def info(args, cache: HTTPCache):
    pprint(cache.get_info(url_pattern=args.url))


def filter_(args, cache: HTTPCache):
    if args.dest_cachefile is not None:
        if os.path.isfile(args.dest_cachefile):
            if (
                input(
                    f"Cache file '{args.cachefile}' already exists! Add to this cache? "
                    "['Yes' to add to the existing cache]: "
                )
                != "Yes"
            ):
                raise ValueError("Dest cache file exists!")
        dest_cache = HTTPCache(
            filename=args.dest_cachefile,
            verbose=args.verbose,
            debug=args.debug,
            dont_expire=True,
            store_as_compressed=args.compressed,
        )
    else:
        dest_cache = None

    if (args.dt_start is not None) or (args.dt_end is not None):
        dt_range = (du_parser.parse(args.dt_start) if args.dt_start is not None else None), (
            du_parser.parse(args.dt_end) if args.dt_end is not None else None
        )
    else:
        dt_range = None

    if (dt_range is None) and args.url is None:
        raise ValueError("--url, --dt_start or --dt_end must be specified")

    urls = cache.filter(
        url_pattern=args.url, dt_range=dt_range, dest_cache=dest_cache, delete=args.delete
    )

    if args.verbose:
        print(f"Filter found the following {len(urls)} urls:")
        print("\n".join(urls))
    else:
        print(f"{len(urls)} urls found")

    if dest_cache:
        print(f"Cache with content for urls is now at '{args.dest_cachefile}'")
        if args.verbose:
            print("New cache info:")
            info = dest_cache.get_info()
            pprint(info)


def merge(args, cache: HTTPCache):
    """
    merge a cache into this cache

    cache - the dest cache that data will be merged to
    """
    if not os.path.isfile(args.other_cachefile):
        raise FileNotFoundError(f"Cache file '{args.cachefile}' not found!")

    other_cache = HTTPCache(
        filename=args.other_cachefile,
        verbose=args.verbose,
        debug=args.debug,
        dont_expire=True,
        store_as_compressed=args.compressed,
    )

    if args.verbose:
        print("Pre merge information:")
        info = cache.get_info()
        print(f"Info for '{args.cachefile}':")
        pprint(info)
        info = other_cache.get_info()
        print(f"Info for '{args.other_cachefile}':")
        pprint(info)

    merged_urls, conflict_urls = cache.merge(other_cache, conflict_mode=args.conflict)

    print(
        "Merge of '{}' into '{}' complete.\n{} urls merged\n{} conflicts".format(
            args.other_cachefile, args.cachefile, len(merged_urls), len(conflict_urls)
        )
    )
    if args.verbose:
        print(f"Final info for '{args.cachefile}':")
        info = cache.get_info()

        print("Merged urls:")
        print("\n".join(merged_urls))
        print("\nConflict urls:")
        print("\n".join(conflict_urls))


def get(args, cache: HTTPCache):
    content = cache.get(args.url)
    if content is None:
        print(f"'{args.url}' not found as a URL in cache")
        content = cache.get(args.url, ident_type="key")
    if content is None:
        print(f"'{args.url}' not found as a cache-key")
        return

    print(content.decode())
    print("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tools for managing onhttpresp caches")
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--debug", action="store_true", default=False)
    parser.add_argument("--uncompressed", dest="compressed", default=True, action="store_false")
    parser.add_argument("cachefile")
    func_parsers = parser.add_subparsers(
        title="function", dest="func_name", description="Function to perform"
    )

    info_parser = func_parsers.add_parser("info", help="Get cache information")
    info_parser.set_defaults(func=info)
    info_parser.add_argument("--url", help="A url pattern to filter for. Supports glob syntax")

    filter_parser = func_parsers.add_parser("filter", help="Filter the cache")
    filter_parser.set_defaults(func=filter_)
    filter_parser.add_argument("--url", help="A url pattern to filter for. Supports glob syntax")
    filter_parser.add_argument(
        "--dt_start", help="Start datetime for filter (inclusive). Format YYYYMMDD HHMMSS"
    )
    filter_parser.add_argument(
        "--dt_end", help="End datetime for filter (exclusive). Format YYYYMMDD HHMMSS"
    )
    filter_parser.add_argument(
        "--dest_cachefile", help="Export data that matches the filter to a new cache at this path"
    )
    filter_parser.add_argument(
        "--delete", action="store_true", default=False, help="Delete urls that match the filter"
    )

    merge_parser = func_parsers.add_parser("merge", help="Merge caches")
    merge_parser.set_defaults(func=merge)
    merge_parser.add_argument(
        "other_cachefile", help="The cache containing the additional content."
    )
    merge_parser.add_argument(
        "--conflict",
        default=CONFLICT_MODE_FAIL,
        choices=(CONFLICT_MODE_SKIP, CONFLICT_MODE_FAIL, CONFLICT_MODE_OVERWRITE),
        help=(
            "Modes that define how to handle merge conflicts. "
            "{0} - keep the old value. {1} - overwrite with the new value. "
            "{2} - Fail the merge process and exit. Default is '{2}'"
        ).format(CONFLICT_MODE_SKIP, CONFLICT_MODE_OVERWRITE, CONFLICT_MODE_FAIL),
    )

    merge_parser = func_parsers.add_parser("get", help="Get content")
    merge_parser.set_defaults(func=get)
    merge_parser.add_argument("url", help="A url to retrieve from the cache")

    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.error("Nothing to do! An operation is required!")

    if not os.path.isfile(args.cachefile):
        parser.error(f"Cache file '{args.cachefile}' not found!")

    cache = HTTPCache(
        filename=args.cachefile,
        verbose=args.verbose,
        debug=args.debug,
        dont_expire=True,
        store_as_compressed=args.compressed,
    )
    print("working...")
    args.func(args, cache)
    print("done")
