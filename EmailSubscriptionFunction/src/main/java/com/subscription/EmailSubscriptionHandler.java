package com.subscription;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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
      context.getLogger().log("Attempting to send response to CloudFormation URL: " + responseUrl);

      Map<String, Object> responseBody = new HashMap<>();
      responseBody.put("Status", status);
      responseBody.put("Reason", reason);
      responseBody.put("PhysicalResourceId", logicalResourceId);
      responseBody.put("StackId", stackId);
      responseBody.put("RequestId", requestId);
      responseBody.put("LogicalResourceId", logicalResourceId);
      responseBody.put("Data", new HashMap<>());

      String jsonResponse = objectMapper.writeValueAsString(responseBody);
      context.getLogger().log("Response payload: " + jsonResponse);

      byte[] responseBytes = jsonResponse.getBytes(StandardCharsets.UTF_8);
      context.getLogger().log("Response size: " + responseBytes.length + " bytes");

      URL url = new URL(responseUrl);
      context
          .getLogger()
          .log(
              "Parsed URL - Protocol: "
                  + url.getProtocol()
                  + ", Host: "
                  + url.getHost()
                  + ", Port: "
                  + url.getPort());

      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("PUT");
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setRequestProperty("Content-Length", String.valueOf(responseBytes.length));
      connection.setRequestProperty("User-Agent", "AWS Lambda Java");
      connection.setConnectTimeout(60000);
      connection.setReadTimeout(60000);
      connection.setDoOutput(true);
      connection.setInstanceFollowRedirects(false);

      context.getLogger().log("Connection configured, attempting to connect...");

      try (OutputStream os = connection.getOutputStream()) {
        os.write(responseBytes);
        os.flush();
        context.getLogger().log("Request body written successfully");
      }

      int responseCode = connection.getResponseCode();
      String responseMessage = connection.getResponseMessage();
      context
          .getLogger()
          .log("HTTP Response - Code: " + responseCode + ", Message: " + responseMessage);

      try {
        java.io.InputStream responseStream =
            (responseCode >= 200 && responseCode < 300)
                ? connection.getInputStream()
                : connection.getErrorStream();

        if (responseStream != null) {
          String responseBody2 = new String(responseStream.readAllBytes(), StandardCharsets.UTF_8);
          context.getLogger().log("Response body: " + responseBody2);
        }
      } catch (Exception e) {
        context.getLogger().log("Could not read response body: " + e.getMessage());
      }

      if (responseCode < 200 || responseCode >= 300) {
        String errorMsg =
            "CloudFormation HTTP error - Code: " + responseCode + ", Message: " + responseMessage;
        context.getLogger().log(errorMsg);
        throw new RuntimeException(errorMsg);
      } else {
        context.getLogger().log("Successfully sent response to CloudFormation");
      }

    } catch (java.net.UnknownHostException e) {
      String errorMsg = "DNS resolution failed for CloudFormation URL: " + e.getMessage();
      context.getLogger().log(errorMsg);
      throw new RuntimeException(errorMsg, e);
    } catch (java.net.ConnectException e) {
      String errorMsg = "Connection failed to CloudFormation URL: " + e.getMessage();
      context.getLogger().log(errorMsg);
      throw new RuntimeException(errorMsg, e);
    } catch (java.net.SocketTimeoutException e) {
      String errorMsg = "Timeout connecting to CloudFormation URL: " + e.getMessage();
      context.getLogger().log(errorMsg);
      throw new RuntimeException(errorMsg, e);
    } catch (IOException e) {
      String errorMsg = "IO error sending response to CloudFormation: " + e.getMessage();
      context.getLogger().log(errorMsg);
      e.printStackTrace();
      throw new RuntimeException(errorMsg, e);
    } catch (Exception e) {
      String errorMsg =
          "Unexpected error creating/sending CloudFormation response: " + e.getMessage();
      context.getLogger().log(errorMsg);
      e.printStackTrace();
      throw new RuntimeException(errorMsg, e);
    }
  }

  private Map<String, Object> createResponse(String status, String message) {
    Map<String, Object> response = new HashMap<>();
    response.put("statusCode", "SUCCESS".equals(status) ? 200 : 500);
    response.put("body", message);
    return response;
  }
}
