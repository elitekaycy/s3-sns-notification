# S3-SNS Event-Driven Architecture with AWS SAM

This project implements a serverless event-driven architecture using AWS services that automatically sends email notifications to multiple recipients when files are uploaded to an S3 bucket.

## Architecture Overview

The system consists of:
- **Amazon S3 Bucket**: Storage for uploaded files with event notifications
- **AWS Lambda Functions**: 
  - Java-based S3 event processor
- **Amazon SNS Topic**: Message distribution service for multiple email notifications
- **IAM Roles**: Secure permissions for cross-service communication

## Features

- ✅ **Serverless Architecture**: No servers to manage
- ✅ **Event-Driven**: Responds automatically to S3 uploads
- ✅ **Multiple Email Recipients**: Supports comma-separated email lists
- ✅ **Environment Separation**: Separate dev/prod environments
- ✅ **CI/CD Pipeline**: Automated deployment with GitHub Actions
- ✅ **Infrastructure as Code**: All resources defined in SAM template
- ✅ **Java 17 Runtime**: Modern Java implementation for all functions
- ✅ **EU Central 1 Region**: Deployed in Frankfurt region
- ✅ **Comprehensive Logging**: CloudWatch integration for monitoring


### Environment-Specific Resources

Resources are created with environment-specific naming:

- **Dev Environment**:
  - Stack: `s3-sns-notification-dev`
  - Bucket: `s3-sns-notification-dev-uploads-dev-{account-id}`
  - SNS Topic: `s3-sns-notification-dev-notifications-dev`

- **Prod Environment**:
  - Stack: `s3-sns-notification-prod`
  - Bucket: `s3-sns-notification-prod-uploads-prod-{account-id}`
  - SNS Topic: `s3-sns-notification-prod-notifications-prod`

## Lambda Function Details

### S3EventProcessor Function
- **Runtime**: Java 17
- **Memory**: 512 MB
- **Timeout**: 30 seconds
- **Handler**: `com.event.S3EventProcessor::handleRequest`

**Key Features**:
- Processes S3 `ObjectCreated:*` events
- Retrieves object metadata (size, content-type, etc.)
- Formats comprehensive notification messages
- Publishes to SNS topic with error handling

### Environment Variables:
- `SNS_TOPIC_ARN`: Target SNS topic for notifications
- `ENVIRONMENT`: Current environment (dev/prod)
- `AWS_REGION`: AWS region (eu-central-1)
