/*
 * Copyright 2016, Ruediger Gad
 * 
 * This software is released under the terms of the Eclipse Public License
 * (EPL) 1.0. You can find a copy of the EPL at:
 * http://opensource.org/licenses/eclipse-1.0.php
 * 
 */
package rgad.bowerick_java_example.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import bowerick.JmsConsumerCallback;
import bowerick.JmsController;
import bowerick.JmsProducer;

/**
 * 
 * Simple example for illustrating the usage of bowerick, using the example of
 * MQTT, from within Java.
 * 
 * @author Ruediger Gad &lt;r.c.g@gmx.de&gt;
 *
 */
public class SimpleSingleTransportMqttTest {

    private static final String TRANSPORT_URL = "mqtt://127.0.0.1:1701";
    private static final String TEST_TOPIC = "/topic/test.topic.foo";

    private JmsController jmsController;

    @Before
    public void setUp() {
        jmsController = new JmsController(TRANSPORT_URL);
        jmsController.startEmbeddedBroker();
    }

    @After
    public void tearDown() {
        jmsController.stopEmbeddedBroker();
    }

    @Test
    public void sendAndReceiveStringViaJson() throws Exception {
        final CountDownLatch cdl = new CountDownLatch(1);
        final StringBuffer received = new StringBuffer();

        AutoCloseable consumer = JmsController.createJsonConsumer(TRANSPORT_URL, TEST_TOPIC, new JmsConsumerCallback() {

            public void processData(Object data) {
                received.append((String) data);
                cdl.countDown();
            }
        }, 1);

        JmsProducer producer = JmsController.createJsonProducer(TRANSPORT_URL, TEST_TOPIC, 1);
        producer.sendData("Test String");

        cdl.await();

        assertEquals("Test String", received.toString());

        producer.close();
        consumer.close();
    }

    @Test
    public void sendAndReceiveListViaJson() throws Exception {
        final CountDownLatch cdl = new CountDownLatch(1);
        final List<Object> received = new ArrayList<>();

        AutoCloseable consumer = JmsController.createJsonConsumer(TRANSPORT_URL, TEST_TOPIC, new JmsConsumerCallback() {

            public void processData(Object data) {
                received.addAll((List<?>) data);
                cdl.countDown();
            }
        }, 1);

        JmsProducer producer = JmsController.createJsonProducer(TRANSPORT_URL, TEST_TOPIC, 1);

        List<Object> testData = new ArrayList<>();
        testData.add("Test String");
        testData.add(42);
        testData.add(1.23456789);
        testData.add(true);

        producer.sendData(testData);

        cdl.await();

        assertNotSame(testData, received);
        assertEquals(testData, received);

        producer.close();
        consumer.close();
    }

    @Test
    public void sendAndReceiveMapViaJson() throws Exception {
        final CountDownLatch cdl = new CountDownLatch(1);
        final Map<String, Object> received = new HashMap<>();

        AutoCloseable consumer = JmsController.createJsonConsumer(TRANSPORT_URL, TEST_TOPIC, new JmsConsumerCallback() {

            public void processData(Object data) {
                received.putAll((Map<String, ?>) data);
                cdl.countDown();
            }
        }, 1);

        JmsProducer producer = JmsController.createJsonProducer(TRANSPORT_URL, TEST_TOPIC, 1);

        Map<String, Object> testData = new HashMap<>();
        testData.put("SomeString", "Test String");
        testData.put("SomeInt", 42);
        testData.put("SomeFloat", 1.23456789);
        testData.put("SomeBoolean", true);

        producer.sendData(testData);

        cdl.await();

        assertNotSame(testData, received);
        assertEquals(testData, received);

        producer.close();
        consumer.close();
    }
}
