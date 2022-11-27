package com.kafkacon.kafkacon.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.kafkacon.kafkacon.models.Response;
import com.kafkacon.kafkacon.models.TopicInfo;

import io.netty.handler.timeout.TimeoutException;

@Service
public class TopicService {
    @Autowired
    BrokerService brokerService;

    public Response<List<TopicInfo>> getTopics(String broker) throws InterruptedException, ExecutionException {
        AdminClient client = brokerService.getClient(broker);
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult listTopics = client.listTopics(options);
        List<TopicInfo> data = listTopics.listings().get().stream()
            .map(topic -> TopicInfo.builder()
                .name(topic.name())
                .internal(topic.isInternal())
            .build())
        .collect(Collectors.toList());
        Response response = Response.builder()
            .data(data)
        .build();
        return response;
    }

    public Response<String> createTopic(String broker, NewTopic topicOptions) {
        AdminClient client = brokerService.getClient(broker);
        Response response = null;
        try {
            client.createTopics(Collections.singleton(topicOptions));
            response = Response.builder()
                .data("Successfully created topic " + topicOptions.name())
            .build();
        } catch (Exception e) {
            response = Response.builder()
                .data("Failed to delete topic. Error: " + e.getMessage())
            .build();
        }
        return response;
        
    }

    public Response<String> deleteTopic(String broker, String topic) {
        AdminClient client = brokerService.getClient(broker);
        Response response = null;
        try {
            client.deleteTopics(Collections.singleton(topic));
            response = Response.builder()
                .data("Successfully deleted topic " + topic)
            .build();
        } catch (Exception e) {
            response = Response.builder()
                .data("Failed to delete topic. Error: " + e.getMessage())
            .build();
        }
        return response;
        
    }

    public Response<Set<String>> getTopicConsumers(String broker, String topic) throws InterruptedException, ExecutionException {
        AdminClient client = brokerService.getClient(broker);
        Set<String> consumers = new HashSet<>();
        client.listConsumerGroups().all().get()
            .stream()
            .forEach(group -> {
                Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                try {
                    map = client.listConsumerGroupOffsets(group.groupId())
                            .partitionsToOffsetAndMetadata()
                            .get();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                catch (ExecutionException e) {
                    e.printStackTrace();
                }
                catch (TimeoutException e) {
                    e.printStackTrace();
                }
                for (TopicPartition topicPartition : map.keySet()) {
                    if (topic.equals(topicPartition.topic())) {
                        consumers.add(group.groupId());
                    }
                }
            });
        Response response = Response.builder()
            .data(consumers)
            .build();
        return response;

        
    }
}
