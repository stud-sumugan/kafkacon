package com.kafkacon.kafkacon.controllers;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafkacon.kafkacon.models.Response;
import com.kafkacon.kafkacon.models.TopicInfo;
import com.kafkacon.kafkacon.services.TopicService;


@RestController
@RequestMapping("/api/v1/topic")
public class TopicController {
    @Autowired
    TopicService topicService;

    @GetMapping
    public Response<List<TopicInfo>> getTopics(@RequestParam String broker) throws InterruptedException, ExecutionException {
        return topicService.getTopics(broker);
    }

    @GetMapping("/{topic}/delete")
    public Response<String> deleteTopic(@RequestParam String broker, @PathVariable String topic) {
        return topicService.deleteTopic(broker, topic);
    }

    @GetMapping("/{topic}/consumers")
    public Response<List<String>> getTopicConsumers(@RequestParam String broker, @PathVariable String topic) {
        
        return null;
    }
}
