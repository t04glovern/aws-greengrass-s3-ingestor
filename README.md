# Greengrass Batch To S3 Component

This component takes a stream of JSON messages from StreamManager and batches them into a gzip file. It uses a JSON Line (JSONL) format for the messages.

For a more detailed explanation of how this component works, see [DETAILS.md](DETAILS.md).

## Sample Configuration

By default, this component will take the JSON message stream from the `BatchMessageStream` stream and batch it into a gzip file every 30 seconds. If configured with `gzip` as the output folder, the gzip files will be written to `/greengrass/v2/work/com.devopstar.json.gzip/gzip/` on the device - This might change depending on the installation path of Greengrass.

The BatchSize configuration is the minimum number of messages that will be batched into a gzip file. If there are not enough messages in the stream, the gzip file will not be created.

The Maximum number of messages that will be batched into a gzip file is 10 times the BatchSize.

### YAML example

```yaml
ComponentDependencies:
  aws.greengrass.StreamManager:
    VersionRequirement: ^2.0.0
    DependencyType: HARD
ComponentConfiguration:
  DefaultConfiguration:
    Path: "/tmp/greengrass/gzip"
    Interval: "10"
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
      "Path": "/tmp/greengrass/gzip",
      "Interval": "10",
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

## Development & Testing

```bash
python3 -m venv .venv
source ./.venv/bin/activate
pip3 install -r requirements-dev.txt
pytest
```

## Publish Component

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

## Roadmap

- [ ] August 2023 | Generalize the component to allow for different input and output formats. Right now this component only supports JSONL input and gzip output. I plan to rename this component to `greengrass-batch-to-s3 - com.devopstar.batch.to.s3` once I've confirmed that multiple inputs and outputs are feasible and worth doing. An example of an output format that I would like to support is Parquet.
- [X] August 2023 | Remove the need for specifying the Path and Interval twice. Right now the Path and Interval are specified in the Processor and Uploader sections of the configuration. I would like to remove the need for this duplication.
- [X] August 2023 | CI/CD pipeline for automated deployment of the component versions to my AWS account.
- [X] September 2023 | Integrate [AWS Greengrass test framework](https://github.com/aws-greengrass/aws-greengrass-testing) into the CI/CD pipeline.
