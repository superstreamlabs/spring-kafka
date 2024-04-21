/*
 * Copyright 2021-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.retrytopic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Tomaz Fernandes
 * @since 2.8.3
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = RetryTopicLegacyFactoryConfigurerIntegrationTests.FIRST_TOPIC, partitions = 1)
public class RetryTopicLegacyFactoryConfigurerIntegrationTests {

	private static final Logger logger = LoggerFactory.getLogger(RetryTopicLegacyFactoryConfigurerIntegrationTests.class);

	public final static String FIRST_TOPIC = "myRetryTopic1";

	@Autowired
	private KafkaTemplate<String, String> sendKafkaTemplate;

	@Autowired
	private CountDownLatchContainer latchContainer;

	@Test
	void shouldRetryFirstAndSecondTopics() {
		logger.debug("Sending message to topic " + FIRST_TOPIC);
		sendKafkaTemplate.send(FIRST_TOPIC, "Testing topic 1");
		assertThat(awaitLatch(latchContainer.countDownLatch1)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchDltOne)).isTrue();
	}

	private boolean awaitLatch(CountDownLatch latch) {
		try {
			return latch.await(150, TimeUnit.SECONDS);
		}
		catch (Exception e) {
			fail(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	@Component
	static class RetryableKafkaListener {

		@Autowired
		CountDownLatchContainer countDownLatchContainer;

		@RetryableTopic(
				attempts = "4",
				backoff = @Backoff(delay = 1000, multiplier = 2.0),
				autoCreateTopics = "false",
				topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
		@KafkaListener(topics = RetryTopicLegacyFactoryConfigurerIntegrationTests.FIRST_TOPIC)
		public void listen(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			countDownLatchContainer.countDownLatch1.countDown();
			logger.warn(in + " from " + topic);
			throw new RuntimeException("test");
		}

		@DltHandler
		public void dlt(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			countDownLatchContainer.countDownLatchDltOne.countDown();
			logger.warn(in + " from " + topic);
		}
	}

	@Component
	static class CountDownLatchContainer {

		CountDownLatch countDownLatch1 = new CountDownLatch(4);
		CountDownLatch countDownLatchDltOne = new CountDownLatch(1);
		CountDownLatch customizerLatch = new CountDownLatch(6);
	}

	@EnableKafka
	@Configuration
	static class Config {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		CountDownLatchContainer latchContainer() {
			return new CountDownLatchContainer();
		}

		@Bean
		RetryableKafkaListener retryableKafkaListener() {
			return new RetryableKafkaListener();
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory, CountDownLatchContainer latchContainer) {

			ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			ContainerProperties props = factory.getContainerProperties();
			props.setIdleEventInterval(100L);
			props.setPollTimeout(50L);
			props.setIdlePartitionEventInterval(100L);
			factory.setConsumerFactory(consumerFactory);
			factory.setConcurrency(1);
			factory.setContainerCustomizer(
					container -> latchContainer.customizerLatch.countDown());
			return factory;
		}

		@Bean
		public ProducerFactory<String, String> producerFactory() {
			Map<String, Object> configProps = new HashMap<>();
			configProps.put(
					ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
					this.broker.getBrokersAsString());
			configProps.put(
					ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					StringSerializer.class);
			configProps.put(
					ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					StringSerializer.class);
			return new DefaultKafkaProducerFactory<>(configProps);
		}

		@Bean
		public KafkaTemplate<String, String> kafkaTemplate() {
			return new KafkaTemplate<>(producerFactory());
		}

		@SuppressWarnings("deprecation")
		@Bean(name = RetryTopicInternalBeanNames.RETRY_TOPIC_CONFIGURER)
		public RetryTopicConfigurer retryTopicConfigurer(DestinationTopicProcessor destinationTopicProcessor,
														ListenerContainerFactoryResolver containerFactoryResolver,
														ListenerContainerFactoryConfigurer listenerContainerFactoryConfigurer,
														BeanFactory beanFactory,
														RetryTopicNamesProviderFactory retryTopicNamesProviderFactory) {
			RetryTopicConfigurer retryTopicConfigurer = new RetryTopicConfigurer(destinationTopicProcessor, containerFactoryResolver, listenerContainerFactoryConfigurer, beanFactory, retryTopicNamesProviderFactory);
			retryTopicConfigurer.useLegacyFactoryConfigurer(true);
			return retryTopicConfigurer;
		}

		@Bean
		public ConsumerFactory<String, String> consumerFactory() {
			Map<String, Object> props = new HashMap<>();
			props.put(
					ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
					this.broker.getBrokersAsString());
			props.put(
					ConsumerConfig.GROUP_ID_CONFIG,
					"groupId");
			props.put(
					ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					StringDeserializer.class);
			props.put(
					ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					StringDeserializer.class);
			props.put(
					ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			return new DefaultKafkaConsumerFactory<>(props);
		}
	}
}
