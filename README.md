# akka-streams-s3-upload

Showcases how to upload to AWS S3 from a Typed Actor Source with backpressure, by using a Graph to process the data.
This stream creates n files, where n is the number of records desired in each file to be uploaded to S3

Provide your AWS credentials as environment variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`) and run it with `sbt run`

The bucket name can be controlled with the `AWS_BUCKET` environment variable
