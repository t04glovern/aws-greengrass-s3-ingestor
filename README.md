# Greengrass S3 Ingestor

The Greengrass S3 Ingestor component takes a stream of JSON messages from StreamManager and batches them into a gzip file. It uses a JSON Line (JSONL) format for the messages. The component enables efficient ingestion of data into S3 for further processing or storage.

For a more detailed explanation of how this component works, see [DETAILS.md](DETAILS.md).

## Operating system

This component can be installed on core devices that run the following operating systems:

* Linux
* Windows

## Requirements

This component has the following requirements:

* The [Greengrass device role](https://docs.aws.amazon.com/greengrass/v2/developerguide/device-service-role.html) must allow the following on the configured S3 bucket - When using a prefix, the bucket resource should include the prefix as well, e.g. `arn:aws:s3:::DOC-EXAMPLE-BUCKET/DOC-EXAMPLE-PREFIX/*`:

  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "s3:PutObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ],
        "Resource": [
          "arn:aws:s3:::DOC-EXAMPLE-BUCKET/*"
        ]
      }
    ]
  }
  ```

## Dependencies

This component has the following dependencies:

* **aws.greengrass.StreamManager**: VersionRequirement: ^2.0.0

## Configuration

This component provides the following configuration parameters that you can customize when deploying the Greengrass S3 Ingestor component.

* `Path` - (Optional) Specifies the location where in-flight data is stored.
  * **Default**: data - which results in data landing in the following location `/greengrass/v2/work/com.devopstar.S3Ingestor/data`, subject to the install location of greengrass on your device.
  * **Example**: If you want to change the storage location to `/tmp/com.devopstar.S3Ingestor`, set Path as `/tmp/com.devopstar.S3Ingestor`.

* `Interval` - (Optional) Determines the time interval (in seconds) after which messages are batched into a gzip file.
  * **Default**: `30`

* `Processor` - Configuration parameters related to message batching to files.
  * `StreamName` - (Optional) The name of the stream from which the JSON messages are taken. This stream will be created for you as part of the deployment and can be published to by other components.
    * **Default**: `BatchMessageStream`
  * `BatchSize` - (Optional) The minimum number of messages that should be batched into a gzip file. If there aren't enough messages in the stream, the gzip file won't be generated. Furthermore, the maximum number of messages that will be batched into a gzip file is 10 times the `BatchSize`.
    * **Default**: `200`

* `Uploader` - Configuration parameters related to uploading batched files to S3.
  * `BucketName` - Specifies the name of the S3 bucket where batched files are uploaded.
  * `Prefix` - (Optional) Determines the folder prefix in the S3 bucket.
    * **Default**: `""`

* `LogLevel` - (Optional) Defines the logging level for the component operations.
  * **Default**: `INFO`

### YAML example

```yaml
ComponentDependencies:
  aws.greengrass.StreamManager:
    VersionRequirement: ^2.0.0
    DependencyType: HARD
ComponentConfiguration:
  DefaultConfiguration:
    Path: "data"
    Interval: "30"
    Processor:
      StreamName: "BatchMessageStream"
      BatchSize: "200"
    Uploader:
      BucketName: "my-bucket"
      Prefix: "sample-devices"
    LogLevel: "INFO"
```

### JSON example

```json
{
  "ComponentDependencies": {
    "aws.greengrass.StreamManager": {
      "VersionRequirement": "^2.0.0",
      "DependencyType": "HARD"
    }
  },
  "ComponentConfiguration": {
    "DefaultConfiguration": {
      "Path": "data",
      "Interval": "30",
      "Processor": {
        "StreamName": "BatchMessageStream",
        "BatchSize": "200"
      },
      "Uploader": {
        "BucketName": "my-bucket",
        "Prefix": "sample-devices",
      },
      "LogLevel": "INFO"
    }
  }
}
```

## Local Log File

This component logs its operations and any potential issues to:

* `/greengrass/v2/logs/com.devopstar.S3Ingestor.log`

To view this component's logs, run the following command:

```bash
tail -f /greengrass/v2/logs/com.devopstar.S3Ingestor.log
```

## Development & Unit Testing

```bash
python3 -m venv .venv
source ./.venv/bin/activate
pip3 install -r requirements-dev.txt
pytest
```

## Build, Test & Publish Component

```bash
# Build component
gdk component build
gdk test-e2e build

# Run integration tests
export AWS_REGION=ap-southeast-2 
gdk test-e2e run

# Publish component
gdk component publish
```
