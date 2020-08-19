package com.rga.aws.controller;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rga.aws.model.SamsungPhone;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
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

    // TODO: Change queue name
    @SqsListener("sqs_queue.fifo")
    public void process(String json) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        SamsungPhone samsungPhone = objectMapper.readValue(json, SamsungPhone.class);
        log.info("Message received. Samsung phone name '{}' and description '{}'", samsungPhone.getName(), samsungPhone.getDescription());
    }

}
