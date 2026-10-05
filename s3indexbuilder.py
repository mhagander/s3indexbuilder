#!/usr/bin/env python3

import argparse
import base64
from collections import defaultdict
import hashlib
import html
import io
import os
import sys
from typing import cast, Generator, List, Tuple
import urllib.parse
import uuid

import boto3

s3 = boto3.client('s3')
cloudfront = boto3.client('cloudfront')

# CloudFront accepts at most this many paths in a single invalidation
MAX_INVALIDATION_PATHS = 3000


def get_complete_bucket(bucket: str, prefix: str) -> Generator[dict, None, None]:
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
        yield from cast(dict, page.get('Contents', []))


def split_bucket_contents(bucket: str, prefix: str, verbose: bool = False) -> Tuple[dict, dict, List[str]]:
    indexes = {}
    files: dict = defaultdict(list)
    skipped: List[str] = []
    # List with a trailing slash, so that prefix "foo" does not also match "foobar/"
    for n, o in enumerate(get_complete_bucket(bucket, prefix + '/' if prefix else ''), 1):
        if verbose and n % 1000 == 0:
            print("Listed {} objects so far".format(n))
        if o['Key'].startswith('/'):
            # Keys with a leading slash have no place in the directory tree
            # (and their index would be served at a "//" URL), so leave them be.
            skipped.append(o['Key'])
            continue
        (dn, fn) = os.path.split(o['Key'])
        if fn == 'index.html':
            indexes[dn] = o
        elif fn == '':
            # "Folder" placeholder object, as created by the S3 console. Not a
            # file, but the directory should still get an index.
            files.setdefault(dn, [])
        else:
            files[dn].append(o)
    return indexes, files, skipped


def fill_missing_parent_directories(files: dict, prefix: str) -> None:
    add: set = set()
    for k in files.keys():
        dn = os.path.dirname(k)
        while dn and dn != prefix:
            if dn in files or dn in add:
                # Already known, and so are all of its parents
                break
            add.add(dn)
            dn = os.path.dirname(dn)
    if prefix not in files:
        add.add(prefix)
    for k in add:
        files[k] = []


def get_subdirectories(files: dict) -> dict:
    subdirs: dict = defaultdict(list)
    for dn in files.keys():
        if dn != '':
            subdirs[os.path.dirname(dn)].append(os.path.basename(dn))
    return subdirs


def generate_index_for(files: dict, subdirs: dict, directory: str) -> str:
    entries = [(os.path.basename(f['Key']), f['LastModified'], f['Size']) for f in files[directory]]
    entries.extend([(name, None, None) for name in subdirs.get(directory, [])])
    s = io.StringIO()
    s.write("<!DOCTYPE html>\n")
    s.write("<html>\n<head>\n<title>Index of {}/</title>\n".format(html.escape(directory)))
    s.write("<style>table {font-family: monospace;} table td { padding-right: 40px;}</style>\n")
    s.write("</head>\n<body>\n<h1>Index of {}/</h1>\n<hr>\n<table>\n".format(html.escape(directory)))
    if directory != '':
        s.write("<tr><td><a href=\"../\">../</a></td><td></td><td></td></tr>\n")
    for name, date, size in sorted(entries, key=lambda x: x[0]):
        name += '/' if size is None else ''
        s.write("<tr><td><a href=\"{}\">{}</a></td><td>{}</td><td>{}</td></tr>\n".format(
            urllib.parse.quote(name),
            html.escape(name),
            date.strftime("%d-%b-%Y %H:%M") if date is not None else '',
            size if size is not None else '',
        ))
    s.write("</table>\n</body>\n</html>\n")
    return s.getvalue()


if __name__ == "__main__":
    parser = argparse.ArgumentParser("S3 index builder")
    parser.add_argument('bucket', help='Name of bucket')
    parser.add_argument('prefix', help='Path prefix to operate on', nargs='?')
    parser.add_argument('--cfdistribution', type=str,
                        help='CloudFront distribution ID to invalidate to')
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument('--quiet', action='store_true', help='No status messages')
    verbosity.add_argument('--verbose', action='store_true',
                           help='Extra status messages (listing progress, unchanged indexes)')

    args = parser.parse_args()

    # Strip s3:// prefix if present, so the script can be called with or without
    args.bucket = args.bucket.removeprefix('s3://')

    prefix = args.prefix.rstrip('/') if args.prefix else ''

    if not args.quiet:
        print("Listing objects in s3://{}/{}".format(args.bucket, prefix + '/' if prefix else ''))

    indexes, files, skipped = split_bucket_contents(args.bucket, prefix, args.verbose)
    if skipped:
        # Shown even with --quiet, as these objects never get indexed
        print("WARNING: skipped {} objects whose key starts with a slash, e.g. {}".format(
            len(skipped), skipped[0]), file=sys.stderr)
        if args.verbose:
            for k in skipped:
                print("Skipped: {}".format(k))
    if files:
        fill_missing_parent_directories(files, prefix)
    else:
        # Nothing to index, but there can still be old index files to remove
        if not args.quiet:
            print("No files found.")

    if not args.quiet:
        print("Found {} files in {} directories, and {} existing index files".format(
            sum(len(f) for f in files.values()),
            len(files),
            len(indexes),
        ))

    invalidations = set([])
    removed = created = updated = unchanged = 0

    # Look for and remove any extra files
    for i in indexes.keys():
        if i not in files:
            key = '{}/index.html'.format(i) if i else 'index.html'
            s3.delete_object(
                Bucket=args.bucket,
                Key=key,
            )
            if not args.quiet:
                print("Index removed: {}".format(key))
            removed += 1
            invalidations.add('/{}/'.format(i) if i else '/')

    subdirs = get_subdirectories(files)
    for d in files.keys():
        idx = generate_index_for(files, subdirs, d).encode()
        md5 = hashlib.md5(idx)
        md5h = md5.hexdigest()
        if d not in indexes:
            created += 1
            if not args.quiet:
                print("Generate new index file in {}".format(d))
        elif indexes[d]['ETag'].strip('"') != md5h:
            updated += 1
            if not args.quiet:
                print("Update index file in {} (hash from {} to {})".format(
                    d,
                    indexes[d]['ETag'].strip('"'),
                    md5h,
                ))
        else:
            unchanged += 1
            if args.verbose:
                print("Index file in {} is unchanged".format(d))
            continue
        s3.put_object(
            Bucket=args.bucket,
            Key='{}/index.html'.format(d) if d else 'index.html',
            Body=idx,
            ContentMD5=base64.b64encode(md5.digest()).decode(),
            ContentType='text/html; charset=utf-8',
        )

        # Invalidations always start with a leading slash, and we need the trailing directory indicator too
        invalidations.add('/{}/'.format(d) if d else '/')

    if not args.quiet:
        print("Indexes: {} created, {} updated, {} unchanged, {} removed".format(
            created, updated, unchanged, removed))

    if invalidations and args.cfdistribution:
        paths = list(invalidations)
        if len(paths) > MAX_INVALIDATION_PATHS:
            # Too many for one request, so invalidate everything we operate on instead
            paths = ['/{}/*'.format(prefix) if prefix else '/*']

        # Issue cache invalidations to CFN
        if args.verbose:
            print("Invalidating {} paths in distribution {}".format(len(paths), args.cfdistribution))
        cloudfront.create_invalidation(
            DistributionId=args.cfdistribution,
            InvalidationBatch={
                'Paths': {
                    'Quantity': len(paths),
                    'Items': paths,
                },
                'CallerReference': str(uuid.uuid4()),
            },
        )
        if not args.quiet:
            if len(paths) == len(invalidations):
                print("Issued invalidation for {} paths".format(len(paths)))
            else:
                print("Issued invalidation for {} ({} changed paths)".format(paths[0], len(invalidations)))
