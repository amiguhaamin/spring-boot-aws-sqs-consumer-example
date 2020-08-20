package com.rga.aws.controller;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.rga.aws.model.SchoolDetails;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.aws.core.support.documentation.RuntimeUse;
import org.springframework.cloud.aws.messaging.config.annotation.NotificationMessage;
import org.springframework.cloud.aws.messaging.listener.Acknowledgment;
import org.springframework.cloud.aws.messaging.listener.SqsMessageDeletionPolicy;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@Slf4j
public class AmazonSQSController {

    @Autowired
    private AmazonSQSAsync amazonSQSAsync;

    @GetMapping("/greeting/{fname}")
    String testCCPA(@PathVariable(name = "fname") String name) {
        return "Hi " + name + ", Welcome to SQS Consumer!";
    }

    /**
     * Set deletion policy to NEVER so that you can acknowledge the incoming message post processing.
     * If mentioned as ON_SUCCESS, message is not deleted when method throws an exception else message
     * will be deleted. ALWAYS means message will be deleted irrespective of the message processing state
     * With Acknowledgment you will have control at which point the message is ok to be deleted from the queue
     *
     * @param message
     * @param acknowledgment
     * @throws IOException
     */
    @RuntimeUse
    @SqsListener(value = { "${cloud.aws.sqs.name}" }, deletionPolicy = SqsMessageDeletionPolicy.NEVER)
    public void process(@NotificationMessage SchoolDetails message, Acknowledgment acknowledgment) throws IOException {
        try {
//        ObjectMapper objectMapper = new ObjectMapper();
//        SamsungPhone samsungPhone = objectMapper.readValue(json, SamsungPhone.class);
            log.info("Message received. Samsung phone name '{}' and description '{}'", message.getName(), message.getDescription());
            acknowledgment.acknowledge();
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

}
