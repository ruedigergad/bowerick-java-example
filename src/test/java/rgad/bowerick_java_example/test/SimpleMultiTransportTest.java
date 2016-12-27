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
import java.util.Arrays;
import java.util.Collections;
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
import clj_assorted_utils.JavaUtils;

/**
 * 
 * Simple example for illustrating the usage of bowerick with multiple
 * transports from within Java.
 * 
 * @author Ruediger Gad &lt;r.c.g@gmx.de&gt;
 *
 */
public class SimpleMultiTransportTest {

	private static final List<String> TRANSPORT_URLS = Collections.unmodifiableList(Arrays.asList(new String[] {
			"stomp://127.0.0.1:1701", "tcp://127.0.0.1:1864", "ws://127.0.0.1:8472", "mqtt://127.0.0.1:2000" }));
	private static final String TEST_TOPIC = "/topic/test.topic.foo";

	private JmsController jmsController;

	@Before
	public void setUp() {
		jmsController = new JmsController(TRANSPORT_URLS);
		jmsController.startEmbeddedBroker();
	}

	@After
	public void tearDown() {
		jmsController.stopEmbeddedBroker();
	}

	@Test
	public void sendAndReceiveStringViaJson() throws Exception {
		final CountDownLatch cdl = new CountDownLatch(TRANSPORT_URLS.size());

		final List<StringBuffer> received = new ArrayList<>();
		List<AutoCloseable> consumers = new ArrayList<>();
		for (int i = 0; i < TRANSPORT_URLS.size(); i++) {
			received.add(new StringBuffer());
			final int idx = i;

			consumers.add(
					JmsController.createJsonConsumer(TRANSPORT_URLS.get(idx), TEST_TOPIC, new JmsConsumerCallback() {

						public void processData(Object data) {
							received.get(idx).append((String) data);
							cdl.countDown();
						}
					}, 1));
		}

		JmsProducer producer = JmsController.createJsonProducer("stomp://127.0.0.1:1701", TEST_TOPIC, 1);
		producer.sendData("Test String");

		cdl.await();

		for (int i = 0; i < received.size(); i++) {
			assertEquals("Test String", received.get(i).toString());
		}

		producer.close();
		for (AutoCloseable consumer : consumers) {
			consumer.close();
		}
	}

	@Test
	public void sendAndReceiveListViaJson() throws Exception {
		final CountDownLatch cdl = new CountDownLatch(TRANSPORT_URLS.size());

		final List<List<Object>> received = new ArrayList<>();
		List<AutoCloseable> consumers = new ArrayList<>();
		for (int i = 0; i < TRANSPORT_URLS.size(); i++) {
			received.add(new ArrayList<Object>());
			final int idx = i;

			consumers.add(
					JmsController.createJsonConsumer(TRANSPORT_URLS.get(idx), TEST_TOPIC, new JmsConsumerCallback() {

						public void processData(Object data) {
							received.get(idx).addAll((List<?>) JavaUtils.convertFromClojureToJava(data));
							cdl.countDown();
						}
					}, 1));
		}

		JmsProducer producer = JmsController.createJsonProducer("stomp://127.0.0.1:1701", TEST_TOPIC, 1);

		List<Object> testData = new ArrayList<>();
		testData.add("Test String");
		testData.add(42);
		testData.add(1.23456789);
		testData.add(true);

		producer.sendData(testData);

		cdl.await();

		for (int i = 0; i < received.size(); i++) {
			assertNotSame(testData, received.get(i));
			assertEquals(testData, received.get(i));
		}

		producer.close();
		for (AutoCloseable consumer : consumers) {
			consumer.close();
		}
	}

	@Test
	public void sendAndReceiveMapViaJson() throws Exception {
		final CountDownLatch cdl = new CountDownLatch(TRANSPORT_URLS.size());
		final List<Map<String, Object>> received = new ArrayList<>();

		List<AutoCloseable> consumers = new ArrayList<>();
		for (int i = 0; i < TRANSPORT_URLS.size(); i++) {
			received.add(new HashMap<String, Object>());
			final int idx = i;

			consumers.add(
					JmsController.createJsonConsumer(TRANSPORT_URLS.get(idx), TEST_TOPIC, new JmsConsumerCallback() {

						public void processData(Object data) {
							received.get(idx).putAll((Map<String, ?>) JavaUtils.convertFromClojureToJava(data));
							cdl.countDown();
						}
					}, 1));
		}

		JmsProducer producer = JmsController.createJsonProducer("stomp://127.0.0.1:1701", TEST_TOPIC, 1);

		Map<String, Object> testData = new HashMap<>();
		testData.put("SomeString", "Test String");
		testData.put("SomeInt", 42);
		testData.put("SomeFloat", 1.23456789);
		testData.put("SomeBoolean", true);

		producer.sendData(testData);

		cdl.await();

		for (int i = 0; i < received.size(); i++) {
			assertNotSame(testData, received.get(i));
			assertEquals(testData, received.get(i));
		}

		producer.close();
		for (AutoCloseable consumer : consumers) {
			consumer.close();
		}
	}
}
