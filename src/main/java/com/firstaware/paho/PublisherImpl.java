package com.firstaware.paho;

import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Callable;

public class PublisherImpl implements Callable<Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(PublisherImpl.class);
    private String mqttUri;
    private String topic;
    private MqttConnectOptions options;

    public PublisherImpl(String mqttUri, String topic, MqttConnectOptions options) {
        this.mqttUri = mqttUri;
        this.topic = topic;
        this.options = options;
    }

    @Override
    public Integer call() throws Exception {
        IMqttClient publisher = null;
        try {
            System.out.println("Start of publishing " + topic);
            String publisherId = UUID.randomUUID().toString();
            publisher = new MqttClient(this.mqttUri, publisherId);
            publisher.connect(options);
            LOG.debug("Are we connected as client {}", publisher.isConnected());
            call(publisher, topic);
            System.out.println("Published completed for topic " + topic);
        } catch (MqttException e) {
            LOG.error("error publishing to the NS ", e);
        } finally {
            try {
                if (publisher != null) {
                    LOG.debug("Done with client calls disconnecting {}", publisher);
                    publisher.disconnect(200);
                }
            } catch (MqttException e) {
                LOG.error("Error disconnecting connection {}", publisher, e);
            }

        }

        return 1;
    }


    private void call(IMqttClient client, String topic) throws MqttException {
        if (!client.isConnected()) {
            throw new RuntimeException("Not Connected");
        }
        MqttMessage msg = createMessage();
        msg.setQos(0);
        msg.setRetained(true);
        client.publish(topic, msg);
    }

    private void callClearTopic(IMqttClient client, String topic) throws MqttException {
        if (!client.isConnected()) {
            throw new RuntimeException("Not Connected");
        }
        MqttMessage msg = clear();
        msg.setQos(0);
        msg.setRetained(true);
        client.publish(topic, msg);
    }

    private MqttMessage createMessage() {
        String message = "{\"msgId\":1,\"devEUI\":\"647FDA0000000D15\",\"port\":1,\"confirmed\":false,\"data\":\"xwAAADw=\"}";
        byte[] payload = message.getBytes();
        MqttMessage msg = new MqttMessage(payload);
        return msg;
    }

    private MqttMessage clear() {
        byte[] payload = new byte[0];
        MqttMessage msg = new MqttMessage(payload);
        return msg;
    }
}
