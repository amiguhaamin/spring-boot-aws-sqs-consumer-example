package com.rga.aws.config;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.aws.messaging.config.QueueMessageHandlerFactory;
import org.springframework.cloud.aws.messaging.config.annotation.EnableSqs;
import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate;
import org.springframework.cloud.aws.messaging.support.NotificationMessageArgumentResolver;
import org.springframework.cloud.aws.messaging.support.converter.NotificationRequestConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.SimpleMessageConverter;

import java.util.ArrayList;
import java.util.Arrays;
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

    // Create an instance of Amazon SQS w/ Async functionality
    @Bean(name = "amazonSQS", destroyMethod = "shutdown")
    public AmazonSQSAsync amazonSQSAsync() {
        return AmazonSQSAsyncClientBuilder
                .standard()
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .withRegion(awsRegion)
                .build();
    }

    @Bean
    public QueueMessagingTemplate queueMessagingTemplate(
            AmazonSQSAsync amazonSQSAsync) {
        QueueMessagingTemplate queueMessagingTemplate = new QueueMessagingTemplate(amazonSQSAsync);
        return queueMessagingTemplate;
    }

    @Bean
    public QueueMessageHandlerFactory queueMessageHandlerFactory(AmazonSQSAsync amazonSQS, BeanFactory beanFactory) {
        ObjectMapper objectMapper = new ObjectMapper();
        // This is the java time module needed in the mapper (can be read in the question)
        objectMapper.registerModule(new JavaTimeModule());

        QueueMessageHandlerFactory factory = new QueueMessageHandlerFactory();
        factory.setAmazonSqs(amazonSQS);
        factory.setBeanFactory(beanFactory);

        MappingJackson2MessageConverter mappingJackson2MessageConverter = new MappingJackson2MessageConverter();
        mappingJackson2MessageConverter.setSerializedPayloadClass(String.class);
        mappingJackson2MessageConverter.setObjectMapper(objectMapper);
        mappingJackson2MessageConverter.setStrictContentTypeMatch(false);
        // NotificationMsgArgResolver is used to deserialize the “Message” data from SNS Notification
        factory.setArgumentResolvers(Arrays.asList(new NotificationMessageArgumentResolver(mappingJackson2MessageConverter)));

        return factory;
    }

    /*@Bean
    public QueueMessageHandlerFactory queueMessageHandlerFactory(
            AmazonSQSAsync amazonSQS, BeanFactory beanFactory) {

        ObjectMapper objectMapper = new ObjectMapper();
        // This is the java time module needed in the mapper (can be read in the question)
        objectMapper.registerModule(new JavaTimeModule());

        // Wrapped in this
        MappingJackson2MessageConverter jacksonMessageConverter =
                new MappingJackson2MessageConverter();
        jacksonMessageConverter.setSerializedPayloadClass(String.class);
        jacksonMessageConverter.setObjectMapper(objectMapper);
        jacksonMessageConverter.setStrictContentTypeMatch(true);

        // Wrapped in this
        List<MessageConverter> payloadArgumentConverters = new ArrayList<>();
        payloadArgumentConverters.add(jacksonMessageConverter);

        // This is the converter that is invoked on SNS messages on SQS listener
        NotificationRequestConverter notificationRequestConverter =
                new NotificationRequestConverter(jacksonMessageConverter);

        payloadArgumentConverters.add(notificationRequestConverter);
        payloadArgumentConverters.add(new SimpleMessageConverter());

        // It needs to be wrapped in this
        CompositeMessageConverter compositeMessageConverter =
                new CompositeMessageConverter(payloadArgumentConverters);

        // Assert.notNull(amazonSQS);
        // Assert.notNull(beanFactory);
        QueueMessageHandlerFactory factory = new QueueMessageHandlerFactory();
        factory.setAmazonSqs(amazonSQS);
        factory.setBeanFactory(beanFactory);

        // The factory has this method for custom resolvers (can be read in the question)
        factory.setArgumentResolvers(Arrays.asList(
                new NotificationMessageArgumentResolver(compositeMessageConverter)));

        return factory;
    }*/

    /*public void consumeSQSMessage(AmazonSQSAsync amazonSQSAsync) {
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
    }*/
}
