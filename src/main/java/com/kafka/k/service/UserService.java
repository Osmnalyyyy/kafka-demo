package com.kafka.k.service;

import com.kafka.k.controller.UserSaveRequest;
import com.kafka.k.controller.UserUpdateRequest;
import com.kafka.k.model.User;
import com.kafka.k.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.NoSuchElementException;

@Service
@RequiredArgsConstructor
public class UserService {
    private final UserRepository userRepository;

    @KafkaListener(topics = "kafka-binary-beasts-save-user", containerFactory = "kafkaListenerContainerFactory")
    public void userSaveListener(@Payload UserSaveRequest userSaveRequest,
                                 @Header("X-AgentName") String agentName,
                                 @Header(KafkaHeaders.OFFSET) int offset,
                                 @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                 Acknowledgment ack) {

        saveUser(userSaveRequest);
    }

    public void saveUser(UserSaveRequest userSaveRequest) {
        User user = User.builder()
                .age(userSaveRequest.getAge())
                .surname(userSaveRequest.getSurname())
                .username(userSaveRequest.getUsername())
                .build();
        userRepository.save(user);
    }


    @KafkaListener(topics = "kafka-binary-beasts-update-user", containerFactory = "kafkaListenerContainerFactory")
    public void userUpdateListener(@Payload UserUpdateRequest userUpdateRequest,
                                   @Header("X-AgentName") String agentName,
                                   @Header(KafkaHeaders.OFFSET) int offset,
                                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                   Acknowledgment ack) {

        updateUser(userUpdateRequest);
    }

    public void updateUser(UserUpdateRequest userUpdateRequest) {
        User user = getById(userUpdateRequest.getId());
        user.setAge(userUpdateRequest.getAge());
        user.setSurname(userUpdateRequest.getSurname());
        user.setUsername(userUpdateRequest.getUsername());
        userRepository.save(user);
    }

    public User getById(Long id) {
        return userRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("Kullanıcı bulunamadı"));
    }

}
