package com.rga.aws.config;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.aws.messaging.config.annotation.EnableSqs;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Slf4j
@EnableSqs
@Configuration
public class SQSConfig {

    @Value("${cloud.aws.region.static}")
    private String awsRegion;

    @Value("${sqs.queue.arn}")
    private String sqsQueueARN;

    private String endpoint = "";

    // Create an instance of Amazon SNS w/ Async functionality
    @Bean//(name = "amazonSNS", destroyMethod = "shutdown")
    public AmazonSQSAsync amazonSQSAsync() {
        return AmazonSQSAsyncClientBuilder
                .standard()
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .withRegion(awsRegion)
                .build();
    }

    public void consumeSQSMessage(AmazonSQSAsync amazonSQSAsync) {
        String sqsUrl = amazonSQSAsync.getQueueUrl("").getQueueUrl();
        final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsUrl)
                .withMaxNumberOfMessages(1)
                .withWaitTimeSeconds(3);
        while(true) {
            final List<Message> messages = amazonSQSAsync.receiveMessage(receiveMessageRequest).getMessages();
            for (Message messageObject : messages) {
                String message = messageObject.getBody();
                log.info("Received message: " + message);
                deleteMessage(amazonSQSAsync, sqsUrl, messageObject);
            }
        }
    }

    private void deleteMessage(AmazonSQSAsync amazonSQSAsync, String sqsUrl, Message messageObject) {
        final String messageReceiptHandle = messageObject.getReceiptHandle();
        amazonSQSAsync.deleteMessage(new DeleteMessageRequest(sqsUrl, messageReceiptHandle));
    }
}
