package com.subscription;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;

public class EmailSubscriptionHandler
    implements RequestHandler<Map<String, Object>, Map<String, Object>> {

  private final SnsClient snsClient;
  private final ObjectMapper objectMapper;

  public EmailSubscriptionHandler() {
    this.snsClient = SnsClient.create();
    this.objectMapper = new ObjectMapper();
  }

  @Override
  public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
    context.getLogger().log("Processing CloudFormation custom resource request");

    try {
      String requestType = (String) input.get("RequestType");
      String responseUrl = (String) input.get("ResponseURL");
      String stackId = (String) input.get("StackId");
      String requestId = (String) input.get("RequestId");
      String logicalResourceId = (String) input.get("LogicalResourceId");

      @SuppressWarnings("unchecked")
      Map<String, Object> resourceProperties =
          (Map<String, Object>) input.get("ResourceProperties");
      String topicArn = (String) resourceProperties.get("TopicArn");
      @SuppressWarnings("unchecked")
      List<String> emailList = (List<String>) resourceProperties.get("EmailList");

      context.getLogger().log("Request Type: " + requestType);
      context.getLogger().log("Topic ARN: " + topicArn);
      context.getLogger().log("Email List: " + emailList);

      if ("Create".equals(requestType) || "Update".equals(requestType)) {
        for (String email : emailList) {
          email = email.trim();
          if (!email.isEmpty()) {
            subscribeEmail(topicArn, email, context);
          }
        }

        sendResponse(
            responseUrl,
            stackId,
            requestId,
            logicalResourceId,
            "SUCCESS",
            "Email subscriptions created successfully",
            context);

      } else if ("Delete".equals(requestType)) {
        sendResponse(
            responseUrl,
            stackId,
            requestId,
            logicalResourceId,
            "SUCCESS",
            "Email subscriptions cleanup completed",
            context);
      }

      return createResponse("SUCCESS", "Operation completed successfully");

    } catch (Exception e) {
      context.getLogger().log("Error processing request: " + e.getMessage());
      e.printStackTrace();

      try {
        String responseUrl = (String) input.get("ResponseURL");
        String stackId = (String) input.get("StackId");
        String requestId = (String) input.get("RequestId");
        String logicalResourceId = (String) input.get("LogicalResourceId");

        sendResponse(
            responseUrl, stackId, requestId, logicalResourceId, "FAILED", e.getMessage(), context);
      } catch (Exception sendError) {
        context.getLogger().log("Error sending failure response: " + sendError.getMessage());
      }

      return createResponse("FAILED", e.getMessage());
    }
  }

  private void subscribeEmail(String topicArn, String email, Context context) {
    try {
      SubscribeRequest subscribeRequest =
          SubscribeRequest.builder().topicArn(topicArn).protocol("email").endpoint(email).build();

      snsClient.subscribe(subscribeRequest);
      context.getLogger().log("Successfully subscribed email: " + email);

    } catch (Exception e) {
      context.getLogger().log("Error subscribing email " + email + ": " + e.getMessage());
      throw e;
    }
  }

  private void sendResponse(
      String responseUrl,
      String stackId,
      String requestId,
      String logicalResourceId,
      String status,
      String reason,
      Context context) {
    try {
      Map<String, Object> responseBody = new HashMap<>();
      responseBody.put("Status", status);
      responseBody.put("Reason", reason);
      responseBody.put("PhysicalResourceId", logicalResourceId);
      responseBody.put("StackId", stackId);
      responseBody.put("RequestId", requestId);
      responseBody.put("LogicalResourceId", logicalResourceId);
      responseBody.put("Data", new HashMap<>());

      String jsonResponse = objectMapper.writeValueAsString(responseBody);
      context.getLogger().log("Sending response to CloudFormation: " + jsonResponse);

    } catch (Exception e) {
      context.getLogger().log("Error creating response: " + e.getMessage());
    }
  }

  private Map<String, Object> createResponse(String status, String message) {
    Map<String, Object> response = new HashMap<>();
    response.put("statusCode", "SUCCESS".equals(status) ? 200 : 500);
    response.put("body", message);
    return response;
  }
}

