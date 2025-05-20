s3indexbuilder
--------------

Trivial tool to create and update a set of index.html files in an S3
bucket when used for file serving. It avoids changing the files whenever
possible, and also auto-generates invalidations for the cloudfront
distribution that's fronting the bucket, assuming there is one.
