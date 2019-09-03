package com.firstaware.paho;

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ListenerImpl implements Callable<Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(ListenerImpl.class);
    private String mqttUri;
    private MqttConnectOptions options;

    public ListenerImpl(String mqttUri, MqttConnectOptions options) {
        this.mqttUri = mqttUri;
        this.options = options;
    }

    @Override
    public Integer call()  {
        IMqttClient listener = null;
        try {
            System.out.println("Subscribe to a topic app/rx");
            String publisherId = UUID.randomUUID().toString();
            listener = new MqttClient(mqttUri, publisherId);
            listener.connect(options);
            LOG.debug("Are we connected for listening {}", listener.isConnected());
            System.out.println("Connected to server with MqttClient");
            CountDownLatch receivedSignal = new CountDownLatch(1);
            listener.subscribe("app/rx", (topic, msg) -> {
                byte[] payload = msg.getPayload();
                LOG.debug("Topic {} has a payload {}", topic, payload);
                // put in to run tests for you when no logger is setup
                System.out.println("Topic " + topic + " payload "+ payload);
                receivedSignal.countDown();
            });
            // put in to run tests for you when no logger is setup
            System.out.println("Waiting for a message on topic ");
            receivedSignal.await(30, TimeUnit.SECONDS);
        } catch (MqttException | InterruptedException e) {
            LOG.error("error contacting or waiting for a signal ", e);
            // put in to run tests for you when no logger is setup
            System.err.println("Failure on topic ");
            e.printStackTrace();
        } finally {
            try {
                if (listener != null) {
                    LOG.debug("Done listening disconnecting  {}", listener);
                    listener.disconnect(200);
                }
            } catch (MqttException e) {
                LOG.error("Error disconnecting connection {}", listener, e);
            }

        }

        return 1;
    }
}

