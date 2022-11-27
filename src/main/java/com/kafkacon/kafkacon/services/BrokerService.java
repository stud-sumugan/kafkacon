package com.kafkacon.kafkacon.services;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class BrokerService {
    Map<String, AdminClient> clients = new HashMap<>();

    public AdminClient getClient(String broker) {
        if (!clients.containsKey(broker)) {
            Map<String, Object> properties = new HashMap<>();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
            AdminClient client = AdminClient.create(properties);
            clients.put(broker, client);
        }
        AdminClient client = clients.get(broker);
        try {
            client.describeCluster();
        } catch (Exception e) {
            log.info("Exception occurred connecting to broker. Resetting connection.");
            Map<String, Object> properties = new HashMap<>();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
            AdminClient newClient = AdminClient.create(properties);
            clients.put(broker, newClient);
        }
        return clients.get(broker);
    }
}
