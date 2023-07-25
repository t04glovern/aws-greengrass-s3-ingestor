Feature: Testing features of Greengrassv2 GDK_COMPONENT_NAME

    Background:
        Given my device is registered as a Thing
        And my device is running Greengrass

    @Sample
    Scenario: As a developer, I can create a component and deploy it on my device
        When I create an S3 bucket for testing
        And I create a Greengrass deployment with components
            | GDK_COMPONENT_NAME           | GDK_COMPONENT_RECIPE_FILE |
            | com.devopstar.Robocat        | LATEST                    |
        And I update my Greengrass deployment configuration, setting the component GDK_COMPONENT_NAME configuration to:
            """
            {
                "MERGE": {
                    "Processor": {
                        "StreamName": "BatchMessageStream",
                        "BatchSize": "20",
                        "Interval": "1",
                        "Path": "/tmp/greengrass/gzip"
                    },
                    "Uploader": {
                        "BucketName": "${aws.resources:s3:bucket:bucketName}",
                        "Prefix": "robocat",
                        "Interval": "1",
                        "Path": "/tmp/greengrass/gzip/*"
                    },
                    "LogLevel": "INFO"
                }
            }
            """
        And I update my Greengrass deployment configuration, setting the component com.devopstar.Robocat configuration to:
            """
            {
                "MERGE": {
                    "Enabled": "true",
                    "Frequency": "0.1"
                }
            }
            """
        And I deploy the Greengrass deployment configuration
        Then the Greengrass deployment is COMPLETED on the device after 180 seconds
        And the com.devopstar.json.gzip log on the device contains the line "Successfully wrote batch_0 to /tmp/greengrass/gzip/batch_0.jsonl.gz" within 60 seconds
        And I call my custom step
