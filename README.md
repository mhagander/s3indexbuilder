s3indexbuilder
--------------

Trivial tool to create and update a set of index.html files in an S3
bucket when used for file serving. It avoids changing the files whenever
possible, and also auto-generates invalidations for the cloudfront
distribution that's fronting the bucket, assuming there is one.

### Usage

    s3indexbuilder.py [--cfdistribution ID] [--identifier TEXT] [--quiet] BUCKET [PREFIX]

`BUCKET` may be given with or without the `s3://` prefix. `PREFIX` limits
the run to one subtree of the bucket; without it the whole bucket is
indexed.

Options:

- `--cfdistribution ID` — CloudFront distribution to invalidate. One
  invalidation batch is issued for all directories whose index changed.
- `--identifier TEXT` — text placed at the top of the top-level index
  (the one for `PREFIX`, or the bucket root). Useful as a fixed string for
  a monitoring check to look for; subdirectory indexes are not affected.
  HTML is escaped. Default: none.
- `--quiet` — no status messages.

### Behaviour

- Every directory that contains files, or that is an ancestor of one,
  gets an `index.html` listing files (name, mtime, size) and
  subdirectories, sorted by name, with a `../` link except at the top.
- An index is only uploaded when its content differs from what is in the
  bucket (compared by MD5 against the object's ETag), so repeated runs
  are no-ops and do not churn CloudFront.
- `index.html` objects for directories that no longer exist are deleted.
- Adding or changing `--identifier` changes the top-level index and
  therefore rewrites it (and invalidates `/`) once; after that it is
  stable again.

### Requirements

Python 3.9+ and `boto3` (`pip install -r requirements.txt`). AWS
credentials come from the usual boto3 sources (environment, profile,
instance role). The caller needs `s3:ListBucket`, `s3:GetObject`,
`s3:PutObject`, `s3:DeleteObject` on the bucket and, with
`--cfdistribution`, `cloudfront:CreateInvalidation`.

### Example

    s3indexbuilder.py s3://yum-archive.postgresql.org \
        --cfdistribution E1234567890ABC \
        --identifier "yum-archive.postgresql.org holds old archived RPM files"
