#!/usr/bin/env python
import argparse
from pprint import pprint

from onhttpreq.cache import HTTPCache, CONFLICT_MODE_SKIP, CONFLICT_MODE_FAIL, CONFLICT_MODE_OVERWRITE


def info(args, cache):
   pprint(cache.get_info(url_pattern=args.url))


def filter_(args, cache):
   dest_cache = (HTTPCache(filename=args.dest_cachefile, verbose=args.verbose, debug=args.debug,
                           dont_expire=True, store_as_compressed=args.compressed)
                 if args.dest_cachefile is not None else None)

   urls = cache.filter(args.url, dest_cache=dest_cache, delete=args.delete)
   if args.verbose:
      print("Filter found the following {} urls:".format(len(urls)))
      print("\n".join(urls))
   else:
      print("{} urls found".format(len(urls)))

   if dest_cache:
      print("New cache with content for urls is now at '{}'".format(args.dest_cachefile))
      if args.verbose:
         print("New cache info:")
         info = dest_cache.get_info()
         pprint(info)


def merge(args, cache):
   other_cache = HTTPCache(filename=args.dest_cachefile, verbose=args.verbose, debug=args.debug,
                           dont_expire=True, store_as_compressed=args.compressed)

   if args.verbose:
      print("Pre merge information:")
      info = cache.get_info()
      print("Info for '{}':".format(args.cachefile))
      pprint(info)
      info = other_cache.get_info()
      print("Info for '{}':".format(args.other_cachefile))
      pprint(info)

   merged_urls, conflict_urls = cache.merge(other_cache, conflict_mode=args.conflict)

   print("Merge of '{}' into '{}' complete. {} urls merged, {} conflicts".format(
      args.other_cachefile, args.cachefile, len(merged_urls), len(conflict_urls)))
   if args.verbose:
      print("Final info for '{}':".format(args.cachefile))
      info = cache.get_info()

      print("Merged urls:")
      print("\n".join(merged_urls))
      print("\nConflict urls:")
      print("\n".join(conflict_urls))


if __name__ == '__main__':
   parser = argparse.ArgumentParser(description="Tools for managing onhttpresp caches")
   parser.add_argument('--verbose', action="store_true", default=False)
   parser.add_argument('--debug', action="store_true", default=False)
   parser.add_argument('--uncompressed', dest="compressed", default=True, action="store_false")
   parser.add_argument('cachefile')
   func_parsers = parser.add_subparsers(title='function', dest='func_name',
                                        description="Function to perform")

   info_parser = func_parsers.add_parser('info', help="Get cache information")
   info_parser.set_defaults(func=info)
   info_parser.add_argument("--url", help="A url pattern to filter for. Supports glob syntax")

   filter_parser = func_parsers.add_parser('filter', help="Filter the cache")
   filter_parser.set_defaults(func=filter_)
   filter_parser.add_argument("--url", help="A url pattern to filter for. Supports glob syntax")
   filter_parser.add_argument("--dest_cachefile", help="Export data that matches the filter to a new cache at this path")
   filter_parser.add_argument("--delete", action="store_true", default=False,
                              help="Delete urls that match the filter")

   merge_parser = func_parsers.add_parser('merge', help="Merge caches")
   merge_parser.set_defaults(func=merge)
   merge_parser.add_argument("other_cachefile", help="The cache containing the additional content.")
   merge_parser.add_argument("--conflict", default=CONFLICT_MODE_FAIL,
                             choices=(CONFLICT_MODE_SKIP, CONFLICT_MODE_FAIL, CONFLICT_MODE_OVERWRITE),
                             help=("Modes that define how to handle merge conflicts. "
                                   "{0} - keep the old value. {1} - overwrite with the new value. "
                                   "{2} - Fail the merge process and exit. Default is '{2}'").format(
                                      CONFLICT_MODE_SKIP, CONFLICT_MODE_OVERWRITE, CONFLICT_MODE_FAIL))


   args = parser.parse_args()
   if not hasattr(args, 'func'):
      parser.error("Nothing to do! An operation is required!")
   cache = HTTPCache(filename=args.cachefile, verbose=args.verbose, debug=args.debug,
                     dont_expire=True, store_as_compressed=args.compressed)
   print("working...")
   args.func(args, cache)
   print("done")
