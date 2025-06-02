#!/usr/bin/env python3

import argparse
import base64
from collections import defaultdict
import hashlib
import io
import os
import sys
from typing import cast, Generator, Tuple
import uuid

import boto3

s3 = boto3.client('s3')
cloudfront = boto3.client('cloudfront')


def get_complete_bucket(bucket: str, prefix: str) -> Generator[dict, None, None]:
    if prefix:
        r = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    else:
        r = s3.list_objects_v2(Bucket=bucket)
    while True:
        if 'Contents' not in r:
            return
        yield from cast(dict, r['Contents'])
        if not r['IsTruncated']:
            return
        r = s3.list_objects_v2(Bucket=bucket, ContinuationToken=r['NextContinuationToken'])


def split_bucket_contents(bucket: str, prefix: str) -> Tuple[dict, dict]:
    indexes = {}
    files = defaultdict(list)
    for o in get_complete_bucket(bucket, prefix):
        (dn, fn) = os.path.split(o['Key'])
        if fn == 'index.html':
            indexes[dn] = o
        else:
            files[dn].append(o)
    return indexes, files


def fill_missing_parent_directories(files: dict, prefix: str) -> None:
    add = []
    for k in files.keys():
        dn = os.path.dirname(k)
        while dn and dn != prefix:
            if dn not in files and dn not in add:
                add.append(dn)
            dn = os.path.dirname(dn)
    if prefix not in files:
        add.append(prefix)
    for k in add:
        files[k] = []


def generate_index_for(files: dict, directory: str) -> str:
    entries = [(os.path.basename(f['Key']), f['LastModified'], f['Size']) for f in files[directory]]
    entries.extend([(os.path.basename(dn), None, None)
                    for dn in files.keys()
                    if dn != '' and os.path.dirname(dn) == directory])
    s = io.StringIO()
    s.write("<!DOCTYPE html>\n")
    s.write("<html>\n<head>\n<title>Index of {}/</title>\n".format(directory))
    s.write("<style>table {font-family: monospace;} table td { padding-right: 40px;}</style>\n")
    s.write("</head>\n<body>\n<h1>Index of {}/</h1>\n<hr>\n<table>\n".format(directory))
    if directory != '':
        s.write("<tr><td><a href=\"../\">../</a></td><td></td><td></td></tr>\n")
    for name, date, size in sorted(entries, key=lambda x: x[0]):
        s.write("<tr><td><a href=\"{0}\">{0}</a></td><td>{1}</td><td>{2}</td></tr>\n".format(
            name + ('/' if size is None else ''),
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
    parser.add_argument('--quiet', action='store_true', help='No status messages')

    args = parser.parse_args()
    prefix = args.prefix.rstrip('/') if args.prefix else ''

    indexes, files = split_bucket_contents(args.bucket, prefix)
    if not files:
        print("No files found.")
        sys.exit(0)

    fill_missing_parent_directories(files, prefix)

    invalidations = set([])

    # Look for and remove any extra files
    for i in indexes.keys():
        if i not in files:
            key = '{}/index.html'.format(i) if i else 'index.html'
            s3.delete_object(
                Bucket=args.bucket,
                Key=key,
            )
            print("Index removed: {}".format(key))
            invalidations.add('/{}/'.format(i) if i else '/')

    for d in files.keys():
        idx = generate_index_for(files, d).encode()
        md5 = hashlib.md5(idx)
        md5h = md5.hexdigest()
        if d not in indexes:
            if not args.quiet:
                print("Generate new index file in {}".format(d))
        elif indexes[d]['ETag'].strip('"') != md5h:
            if not args.quiet:
                print("Update index file in {} (hash from {} to {})".format(
                    d,
                    indexes[d]['ETag'].strip('"'),
                    md5h,
                ))
        else:
            continue
        s3.put_object(
            Bucket=args.bucket,
            Key='{}/index.html'.format(d) if d else 'index.html',
            Body=idx,
            ContentMD5=base64.b64encode(md5.digest()).decode(),
            ContentType='text/html',
        )

        # Invalidations always start with a leading slash, and we need the trailing directory indicator too
        invalidations.add('/{}/'.format(d) if d else '/')

    if invalidations and args.cfdistribution:
        # Issue cache invalidations to CFN
        cloudfront.create_invalidation(
            DistributionId=args.cfdistribution,
            InvalidationBatch={
                'Paths': {
                    'Quantity': len(invalidations),
                    'Items': list(invalidations),
                },
                'CallerReference': str(uuid.uuid4()),
            },
        )
        if not args.quiet:
            print("Issued invalidation for {} paths".format(len(invalidations)))
