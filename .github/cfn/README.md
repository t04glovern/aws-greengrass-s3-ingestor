# CI/CD Role deployment

This CloudFormation template creates the OIDC-enabled IAM role used by the CI/CD pipeline to deploy the AWS Greengrass component from this repository.

```bash
aws cloudformation deploy \
    --template-file ./.github/cfn/oidc-role.yml \
    --region ap-southeast-2 \
    --stack-name oidc-t04glovern-aws-greengrass-s3-ingestor \
    --capabilities CAPABILITY_NAMED_IAM
```
