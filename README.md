# akka-streams-s3-upload

Showcases how to upload to AWS S3 from a Typed Actor Source, by using a Graph to process the data

Provide your AWS credentials as environment variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`) and run it with `sbt run`

The bucket name can be controlled with the `AWS_BUCKET` environment variable
