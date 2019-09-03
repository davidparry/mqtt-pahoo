package com.firstaware.paho;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PahoService {
    private static final Logger LOG = LoggerFactory.getLogger(PahoService.class);
    public final String topic;
    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    public PahoService(String topic) {
        this.topic = topic;
    }

    public static void main(String[] args) {
        String username = ""; // this needs to be provided still unsure where it comes from
        String password = ""; // this needs to be provided
        String server = "tcp://tek-ns-us.thingsboard.io:1883";
        String topic = "gateway/647FDA8010000000/tx";
        new PahoService(topic).callMqttServer(server, username, password);
    }

    public void callMqttServer(String server, String username, String passwword) {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);
        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(passwword)) {
            options.setUserName(username);
            options.setPassword(passwword.toCharArray());
        }
        PublisherImpl publisher = new PublisherImpl(server, topic, options);
        ListenerImpl listener = new ListenerImpl(server, options);
        executorService.submit(listener);
        sleep(50);
        executorService.submit(publisher);
        try {
            executorService.awaitTermination(5, TimeUnit.MINUTES);
        } catch (Exception er) {
            LOG.info("Message {}", er.getMessage());
        }
    }

    /**
     * Wait for connections not for production just to test
     *
     * @param time
     */
    private void sleep(int time) {
        try {
            Thread.sleep(time);
        } catch (Exception e) {
            LOG.error("Error waiting for listener ", e);
        }


    }

}
