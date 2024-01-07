package com.kafka.k.controller;

import com.kafka.k.model.User;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("api/v1/user")
@RequiredArgsConstructor
public class UserController {
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String topicName = "kafka-binary-beasts-save-user";


    @PostMapping("/save")
    public ResponseEntity<Object> saveUser(@RequestBody UserSaveRequest userSaveRequest) throws ExecutionException, InterruptedException {

        Message<UserSaveRequest> userMsg =
                MessageBuilder.withPayload(userSaveRequest)
                        .setHeader(KafkaHeaders.TOPIC, topicName)
                        // .setHeader(KafkaHeaders.PARTITION, 1)
                        //  .setHeader(KafkaHeaders.GROUP_ID, groupId)
                        .setHeader("X-AgentName", "kafka-demo-app")
                        .build();

        kafkaTemplate.send(userMsg).get();
        return ResponseEntity.ok(userMsg);
    }

    @PostMapping("/update")
    public ResponseEntity<Object> UpdateUser(@RequestBody UserUpdateRequest userUpdateRequest) throws ExecutionException, InterruptedException {

        Message<UserUpdateRequest> userMsg =
                MessageBuilder.withPayload(userUpdateRequest)
                        .setHeader(KafkaHeaders.TOPIC, "kafka-binary-beasts-update-user")
                      //  .setHeader(KafkaHeaders.PARTITION, 1)
                        .setHeader("X-AgentName", "kafka-demo-app")
                        .build();

        kafkaTemplate.send(userMsg).get();
        return ResponseEntity.ok(userMsg);
    }
}
