package com.event;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import java.time.Instant;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

public class S3EventProcessor implements RequestHandler<S3Event, String> {

  private final SnsClient snsClient;
  private final S3Client s3Client;
  private final String snsTopicArn;
  private final String environment;

  public S3EventProcessor() {
    this.snsClient = SnsClient.builder().region(Region.of(System.getenv("AWS_REGION"))).build();
    this.s3Client = S3Client.builder().region(Region.of(System.getenv("AWS_REGION"))).build();
    this.snsTopicArn = System.getenv("SNS_TOPIC_ARN");
    this.environment = System.getenv("ENVIRONMENT");
  }

  @Override
  public String handleRequest(S3Event s3Event, Context context) {
    context.getLogger().log("Received S3 event with " + s3Event.getRecords().size() + " records");

    for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
      try {
        processS3Record(record, context);
      } catch (Exception e) {
        context.getLogger().log("Error processing S3 record: " + e.getMessage());
        throw new RuntimeException("Failed to process S3 event", e);
      }
    }

    return "Successfully processed " + s3Event.getRecords().size() + " S3 records";
  }

  private void processS3Record(
      S3EventNotification.S3EventNotificationRecord record, Context context) {
    String bucketName = record.getS3().getBucket().getName();
    String objectKey = record.getS3().getObject().getKey();
    String eventName = record.getEventName();

    context
        .getLogger()
        .log(
            String.format(
                "Processing %s event for object %s in bucket %s",
                eventName, objectKey, bucketName));

    HeadObjectRequest headObjectRequest =
        HeadObjectRequest.builder().bucket(bucketName).key(objectKey).build();

    HeadObjectResponse headObjectResponse = s3Client.headObject(headObjectRequest);

    String subject = String.format("[%s] New File Uploaded to S3", environment.toUpperCase());
    String message = buildNotificationMessage(bucketName, objectKey, eventName, headObjectResponse);

    sendSnsNotification(subject, message, context);
  }

  private String buildNotificationMessage(
      String bucketName, String objectKey, String eventName, HeadObjectResponse objectMetadata) {
    StringBuilder message = new StringBuilder();
    message.append("File Upload Notification\n");
    message.append("========================\n\n");
    message.append("Event: ").append(eventName).append("\n");
    message.append("Environment: ").append(environment).append("\n");
    message.append("Bucket: ").append(bucketName).append("\n");
    message.append("Object Key: ").append(objectKey).append("\n");
    message
        .append("File Size: ")
        .append(formatFileSize(objectMetadata.contentLength()))
        .append("\n");
    message.append("Content Type: ").append(objectMetadata.contentType()).append("\n");
    message.append("Last Modified: ").append(objectMetadata.lastModified().toString()).append("\n");
    message.append("ETag: ").append(objectMetadata.eTag()).append("\n\n");
    message.append("Upload Time: ").append(Instant.now().toString()).append("\n");

    return message.toString();
  }

  private String formatFileSize(Long sizeInBytes) {
    if (sizeInBytes == null) return "Unknown";

    if (sizeInBytes < 1024) return sizeInBytes + " bytes";
    if (sizeInBytes < 1024 * 1024) return String.format("%.2f KB", sizeInBytes / 1024.0);
    if (sizeInBytes < 1024 * 1024 * 1024)
      return String.format("%.2f MB", sizeInBytes / (1024.0 * 1024.0));
    return String.format("%.2f GB", sizeInBytes / (1024.0 * 1024.0 * 1024.0));
  }

  private void sendSnsNotification(String subject, String message, Context context) {
    try {
      PublishRequest publishRequest =
          PublishRequest.builder().topicArn(snsTopicArn).subject(subject).message(message).build();

      PublishResponse publishResponse = snsClient.publish(publishRequest);

      context
          .getLogger()
          .log("SNS notification sent successfully. MessageId: " + publishResponse.messageId());

    } catch (Exception e) {
      context.getLogger().log("Failed to send SNS notification: " + e.getMessage());
      throw new RuntimeException("Failed to send SNS notification", e);
    }
  }
}

