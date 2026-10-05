s3indexbuilder
--------------

Trivial tool to create and update a set of index.html files in an S3
bucket when used for file serving. It avoids changing the files whenever
possible, and also auto-generates invalidations for the cloudfront
distribution that's fronting the bucket, assuming there is one.

Usage
-----

    s3indexbuilder.py [--cfdistribution ID] [--quiet | --verbose] bucket [prefix]

`bucket` is the name of the bucket, with or without a leading `s3://`.
If `prefix` is given, only that directory and everything below it is
processed. With `--cfdistribution`, the changed directories are
invalidated in that CloudFront distribution. `--quiet` suppresses status
messages, and `--verbose` adds listing progress and unchanged directories.

AWS credentials and region are picked up by boto3 in the usual way.
