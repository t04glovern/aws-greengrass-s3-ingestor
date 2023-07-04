# StreamManager JSON Gzip Component

This component takes a stream of JSON messages from StreamManager and batches them into a gzip file. It uses a JSON Line (JSONL) format for the messages.

## Sample Configuration

By default this component will take the JSON message stream from the `BatchMessageStream` stream and batch it into a gzip file every 30 seconds. If configured with `gzip` as the output folder, the gzip files will be written to `/greengrass/v2/work/com.devopstar.json.gzip/gzip/` on the device - This might changes depending on the installation path of Greengrass.

The BatchSize configuration is the minimum number of messages that will be batched into a gzip file. If there are not enough messages in the stream, the gzip file will not be created.

The Maximum number of messages that will be batched into a gzip file is 10 times the BatchSize.

**YAML example**
```
ComponentDependencies:
  aws.greengrass.StreamManager:
    VersionRequirement: "^2.0.0"
    DependencyType: "HARD"
ComponentConfiguration:
  DefaultConfiguration:
    Processor:
      StreamName: "BatchMessageStream"
      BatchSize: "20"
      Interval: "30"
      Path: "/tmp/greengrass/gzip"
    Uploader:
      BucketName: "my-bucket"
      Prefix: "sample-devices"
      Interval: "1"
      Path: "/tmp/greengrass/gzip/*"
    LogLevel: "INFO"
```

**JSON example**
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
      "Processor": {
        "StreamName": "BatchMessageStream",
        "BatchSize": "20",
        "Interval": "30",
        "Path": "/tmp/greengrass/gzip"
      },
      "Uploader": {
        "BucketName": "my-bucket",
        "Prefix": "sample-devices",
        "Interval": "1",
        "Path": "/tmp/greengrass/gzip/*"
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
gdk component build
gdk component publish
```
