package com.subscription;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.*;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class EmailSubscriptionHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

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
            CustomResourceRequest request = parseRequest(input);
            context.getLogger().log("Request Type: " + request.requestType);
            context.getLogger().log("Topic ARN: " + request.topicArn);
            context.getLogger().log("Email List: " + request.emailList);

            String result = processRequest(request, context);
            
            sendCloudFormationResponse(request, "SUCCESS", result, context);
            return createResponse("SUCCESS", "Operation completed successfully");

        } catch (Exception e) {
            context.getLogger().log("Error processing request: " + e.getMessage());
            e.printStackTrace();

            try {
                CustomResourceRequest request = parseRequest(input);
                sendCloudFormationResponse(request, "FAILED", e.getMessage(), context);
            } catch (Exception sendError) {
                context.getLogger().log("Error sending failure response: " + sendError.getMessage());
            }

            return createResponse("FAILED", e.getMessage());
        }
    }

    private CustomResourceRequest parseRequest(Map<String, Object> input) {
        CustomResourceRequest request = new CustomResourceRequest();
        request.requestType = (String) input.get("RequestType");
        request.responseUrl = (String) input.get("ResponseURL");
        request.stackId = (String) input.get("StackId");
        request.requestId = (String) input.get("RequestId");
        request.logicalResourceId = (String) input.get("LogicalResourceId");

        @SuppressWarnings("unchecked")
        Map<String, Object> resourceProperties = (Map<String, Object>) input.get("ResourceProperties");
        request.topicArn = (String) resourceProperties.get("TopicArn");
        request.emailList = parseEmailList(resourceProperties.get("EmailList"));

        return request;
    }

    private List<String> parseEmailList(Object emailListObj) {
        List<String> emailList = new ArrayList<>();
        
        if (emailListObj instanceof List) {
            @SuppressWarnings("unchecked")
            List<String> emails = (List<String>) emailListObj;
            emailList = emails;
        } else if (emailListObj instanceof String) {
            String emailString = (String) emailListObj;
            if (!emailString.trim().isEmpty()) {
                Arrays.stream(emailString.split(","))
                        .map(String::trim)
                        .filter(email -> !email.isEmpty())
                        .forEach(emailList::add);
            }
        }
        
        return emailList;
    }

    private String processRequest(CustomResourceRequest request, Context context) {
        switch (request.requestType) {
            case "Create":
            case "Update":
                return handleCreateOrUpdate(request, context);
            case "Delete":
                return handleDelete(request, context);
            default:
                throw new IllegalArgumentException("Unknown request type: " + request.requestType);
        }
    }

    private String handleCreateOrUpdate(CustomResourceRequest request, Context context) {
        if (request.emailList.isEmpty()) {
            context.getLogger().log("Warning: No email addresses provided");
            return "No email addresses to subscribe";
        }

        List<String> successfulEmails = new ArrayList<>();
        List<String> failedEmails = new ArrayList<>();

        for (String email : request.emailList) {
            try {
                subscribeEmail(request.topicArn, email, context);
                successfulEmails.add(email);
            } catch (Exception e) {
                context.getLogger().log("Failed to subscribe " + email + ": " + e.getMessage());
                failedEmails.add(email);
            }
        }

        if (successfulEmails.isEmpty()) {
            throw new RuntimeException("Failed to subscribe any emails. Failed: " + failedEmails);
        }

        String result = "Successfully subscribed " + successfulEmails.size() + " email(s)";
        if (!failedEmails.isEmpty()) {
            result += ". Failed to subscribe " + failedEmails.size() + " email(s): " + failedEmails;
        }
        
        return result;
    }

    private String handleDelete(CustomResourceRequest request, Context context) {
        if (request.emailList.isEmpty()) {
            context.getLogger().log("No email addresses to unsubscribe");
            return "No email subscriptions to clean up";
        }

        List<String> unsubscribedEmails = new ArrayList<>();
        List<String> failedEmails = new ArrayList<>();

        for (String email : request.emailList) {
            try {
                unsubscribeEmail(request.topicArn, email, context);
                unsubscribedEmails.add(email);
            } catch (Exception e) {
                context.getLogger().log("Failed to unsubscribe " + email + ": " + e.getMessage());
                failedEmails.add(email);
            }
        }

        String result = "Unsubscribed " + unsubscribedEmails.size() + " email(s)";
        if (!failedEmails.isEmpty()) {
            result += ". Failed to unsubscribe " + failedEmails.size() + " email(s): " + failedEmails;
        }
        
        return result;
    }

    private void subscribeEmail(String topicArn, String email, Context context) {
        context.getLogger().log("Subscribing email: " + email + " to topic: " + topicArn);

        try {
            SubscribeRequest request = SubscribeRequest.builder()
                    .topicArn(topicArn)
                    .protocol("email")
                    .endpoint(email)
                    .build();

            SubscribeResponse response = snsClient.subscribe(request);
            context.getLogger().log("Successfully subscribed " + email + " with ARN: " + response.subscriptionArn());

        } catch (InvalidParameterException e) {
            if (e.getMessage().contains("Invalid parameter: Email address")) {
                throw new RuntimeException("Invalid email address: " + email, e);
            }
            throw new RuntimeException("Invalid parameter for email " + email + ": " + e.getMessage(), e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to subscribe email " + email + ": " + e.getMessage(), e);
        }
    }

    private void unsubscribeEmail(String topicArn, String email, Context context) {
        context.getLogger().log("Unsubscribing email: " + email + " from topic: " + topicArn);

        try {
            String subscriptionArn = findSubscriptionArn(topicArn, email, context);
            if (subscriptionArn == null) {
                context.getLogger().log("No subscription found for email: " + email);
                return;
            }

            UnsubscribeRequest request = UnsubscribeRequest.builder()
                    .subscriptionArn(subscriptionArn)
                    .build();

            snsClient.unsubscribe(request);
            context.getLogger().log("Successfully unsubscribed email: " + email);

        } catch (Exception e) {
            throw new RuntimeException("Failed to unsubscribe email " + email + ": " + e.getMessage(), e);
        }
    }

    private String findSubscriptionArn(String topicArn, String email, Context context) {
        ListSubscriptionsByTopicRequest request = ListSubscriptionsByTopicRequest.builder()
                .topicArn(topicArn)
                .build();

        ListSubscriptionsByTopicResponse response = snsClient.listSubscriptionsByTopic(request);

        for (Subscription subscription : response.subscriptions()) {
            if ("email".equals(subscription.protocol()) && email.equals(subscription.endpoint())) {
                String subscriptionArn = subscription.subscriptionArn();
                
                if ("PendingConfirmation".equals(subscriptionArn)) {
                    context.getLogger().log("Skipping pending confirmation subscription for: " + email);
                    continue;
                }
                
                context.getLogger().log("Found subscription ARN: " + subscriptionArn + " for email: " + email);
                return subscriptionArn;
            }
        }
        
        return null;
    }

    private void sendCloudFormationResponse(CustomResourceRequest request, String status, String reason, Context context) {
        try {
            context.getLogger().log("Sending CloudFormation response: " + status);

            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("Status", status);
            responseBody.put("Reason", reason);
            responseBody.put("PhysicalResourceId", request.logicalResourceId);
            responseBody.put("StackId", request.stackId);
            responseBody.put("RequestId", request.requestId);
            responseBody.put("LogicalResourceId", request.logicalResourceId);
            responseBody.put("Data", new HashMap<>());

            String jsonResponse = objectMapper.writeValueAsString(responseBody);
            byte[] responseBytes = jsonResponse.getBytes(StandardCharsets.UTF_8);

            URL url = new URL(request.responseUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            
            connection.setRequestMethod("PUT");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Content-Length", String.valueOf(responseBytes.length));
            connection.setConnectTimeout(60000);
            connection.setReadTimeout(60000);
            connection.setDoOutput(true);

            try (OutputStream os = connection.getOutputStream()) {
                os.write(responseBytes);
                os.flush();
            }

            int responseCode = connection.getResponseCode();
            context.getLogger().log("CloudFormation response code: " + responseCode);

            if (responseCode < 200 || responseCode >= 300) {
                throw new RuntimeException("CloudFormation HTTP error: " + responseCode);
            }

        } catch (Exception e) {
            context.getLogger().log("Error sending CloudFormation response: " + e.getMessage());
            throw new RuntimeException("Failed to send CloudFormation response", e);
        }
    }

    private Map<String, Object> createResponse(String status, String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("statusCode", "SUCCESS".equals(status) ? 200 : 500);
        response.put("body", message);
        return response;
    }

    private static class CustomResourceRequest {
        String requestType;
        String responseUrl;
        String stackId;
        String requestId;
        String logicalResourceId;
        String topicArn;
        List<String> emailList;
    }
}