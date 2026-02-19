#!/usr/bin/env python

"""
CLI program that cleans up an onhttpreq cache file. This script will remove/sweep
away all unused urls from a cache file. The user specifies on command line
a cache db file and a valid-urls text file. The program then removes from the
db any record with a url not in the valid-urls file.

The program also has a 'dryrun' mode that will just report on the db records that would
be removed if the operation proceeded as normal.
"""

import argparse
import os
from collections import namedtuple
from difflib import SequenceMatcher
from typing import cast

from sqlalchemy import func, select
from tqdm import tqdm

from onhttpreq.cache import HTTPCacheContent, create_sessionmaker

_UrlGroup = namedtuple("_UrlGroup", ["label", "representative", "count", "members"])


def _make_group_label(members, representative, min_prefix_len=10):
    prefix = os.path.commonprefix(members)
    if len(prefix) >= min_prefix_len:
        return prefix + "..."
    return representative[:80] + ("..." if len(representative) > 80 else "")


def group_urls(urls, threshold):
    """Group URLs by text similarity. Returns list of _UrlGroup."""
    groups = []  # list of [representative, [members]]

    for url in tqdm(urls, desc="Grouping URLs", unit="url"):
        best_ratio = 0.0
        best_idx = -1
        for i, (rep, _members) in enumerate(groups):
            ratio = SequenceMatcher(None, url, rep).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_idx = i

        if best_ratio >= threshold and best_idx >= 0:
            groups[best_idx][1].append(url)
        else:
            groups.append([url, [url]])

    result = []
    for rep, members in groups:
        label = _make_group_label(members, rep)
        result.append(
            _UrlGroup(label=label, representative=rep, count=len(members), members=members)
        )

    return sorted(result, key=lambda g: g.count, reverse=True)


def classify_urls_by_groups(urls, groups, threshold):
    """Classify URLs into existing groups. Returns list of _UrlGroup for matched + unmatched."""
    reps = [(g.label, g.representative) for g in groups]
    buckets = {label: [] for label, _ in reps}
    unmatched = []

    for url in tqdm(urls, desc="Classifying URLs", unit="url"):
        best_ratio = 0.0
        best_label = None
        for label, rep in reps:
            ratio = SequenceMatcher(None, url, rep).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_label = label

        if best_ratio >= threshold and best_label is not None:
            buckets[best_label].append(url)
        else:
            unmatched.append(url)

    result = []
    for label, members in buckets.items():
        if members:
            result.append(
                _UrlGroup(label=label, representative="", count=len(members), members=members)
            )
    if unmatched:
        result.extend(group_urls(unmatched, threshold))

    return sorted(result, key=lambda g: g.count, reverse=True)


def _print_url_summary(label, groups):
    total = sum(g.count for g in groups)
    print(f"\n--- {label} ({total} urls, {len(groups)} groups) ---")
    for g in groups:
        print(f"  {g.count:>6}  {g.label}")


def load_valid_urls(filepath):
    print(f"Loading valid urls from {filepath=}")
    with open(filepath) as f:
        urls = set(line.strip() for line in f if line.strip())
    print(f"n={len(urls)} valid urls found in {filepath=}")
    return urls


def process_cli():
    parser = argparse.ArgumentParser(
        description="Remove URLs from an onhttpreq cache DB that are not in a valid-urls file."
    )
    parser.add_argument("cache_db", help="Path to the cache sqlite DB file")
    parser.add_argument("valid_urls_file", help="Path to a text file with one valid URL per line")
    parser.add_argument(
        "--dryrun",
        action="store_true",
        help="Only report which records would be removed, don't actually delete",
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--skip_reporting", action="store_true")
    parser.add_argument(
        "--group_sim_threshold",
        "--reporting_groups_sim",
        default=0.4,
        type=float,
        help="When reporting groups of urls, use this threshold to find similar urls. "
        "Lower threshold means fewer groups. Ignored if --skip_reporting is used. Default=0.4",
    )
    return parser.parse_args()


def main(args):
    print(f"Starting execution {args.dryrun=} {args.verbose=}")

    if not os.path.isfile(args.cache_db):
        raise FileNotFoundError(f"db cache file does not exist at '{args.cache_db}'")

    valid_urls = load_valid_urls(args.valid_urls_file)

    if not args.skip_reporting:
        valid_groups = group_urls(valid_urls, args.group_sim_threshold)
        _print_url_summary("Valid URL groups", valid_groups)

    sessionmaker, _ = create_sessionmaker(args.cache_db)
    session = sessionmaker()

    try:
        total = cast(int, session.execute(select(func.count(HTTPCacheContent.url))).scalar())
        print(f"\nn={total} cache entries found in {args.cache_db=}")
        urls_to_delete = []

        for (row,) in session.execute(select(HTTPCacheContent.url)):
            if row in valid_urls:
                continue
            urls_to_delete.append(row)
            if args.verbose:
                print(f" invalid-url: {row}")

        if not urls_to_delete:
            print("No unused urls were found in the db")
            return

        if not args.skip_reporting:
            delete_groups = classify_urls_by_groups(
                urls_to_delete, valid_groups, args.group_sim_threshold
            )
            _print_url_summary("URLs to delete", delete_groups)

        if args.dryrun:
            print(f"\n[dryrun] Would remove {len(urls_to_delete)} of {total} records.")
            return

        print(f"\nRemoving {len(urls_to_delete)} of {total} cache entries.")
        for url in tqdm(urls_to_delete, desc="Deleting", unit="url"):
            session.query(HTTPCacheContent).filter(HTTPCacheContent.url == url).delete()
        session.commit()
        print(
            f"\nRemoved {len(urls_to_delete)} of {total} records. {total - len(urls_to_delete)} records remain."
        )
    finally:
        session.close()


if __name__ == "__main__":
    args = process_cli()
    main(args)
