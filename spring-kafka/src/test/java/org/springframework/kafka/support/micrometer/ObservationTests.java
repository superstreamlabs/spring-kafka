/*
 * Copyright 2022-2023 the original author or authors.
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

package org.springframework.kafka.support.micrometer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.support.micrometer.KafkaListenerObservation.DefaultKafkaListenerObservationConvention;
import org.springframework.kafka.support.micrometer.KafkaTemplateObservation.DefaultKafkaTemplateObservationConvention;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.lang.Nullable;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import io.micrometer.common.KeyValues;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.tck.MeterRegistryAssert;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.propagation.Propagator;
import io.micrometer.tracing.test.simple.SimpleSpan;
import io.micrometer.tracing.test.simple.SimpleTracer;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 3.0
 */
@SpringJUnitConfig
@EmbeddedKafka(topics = { "observation.testT1", "observation.testT2", "ObservationTests.testT3" })
@DirtiesContext
public class ObservationTests {

	@Test
	void endToEnd(@Autowired Listener listener, @Autowired KafkaTemplate<Integer, String> template,
			@Autowired SimpleTracer tracer, @Autowired KafkaListenerEndpointRegistry rler,
			@Autowired MeterRegistry meterRegistry, @Autowired EmbeddedKafkaBroker broker,
			@Autowired KafkaListenerEndpointRegistry endpointRegistry, @Autowired KafkaAdmin admin,
			@Autowired @Qualifier("customTemplate") KafkaTemplate<Integer, String> customTemplate,
			@Autowired Config config)
					throws InterruptedException, ExecutionException, TimeoutException {

		template.send("observation.testT1", "test").get(10, TimeUnit.SECONDS);
		assertThat(listener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.record).isNotNull();
		Headers headers = listener.record.headers();
		assertThat(headers.lastHeader("foo")).extracting(hdr -> hdr.value()).isEqualTo("some foo value".getBytes());
		assertThat(headers.lastHeader("bar")).extracting(hdr -> hdr.value()).isEqualTo("some bar value".getBytes());
		Deque<SimpleSpan> spans = tracer.getSpans();
		assertThat(spans).hasSize(4);
		SimpleSpan span = spans.poll();
		assertThat(span.getTags()).containsEntry("spring.kafka.template.name", "template");
		assertThat(span.getName()).isEqualTo("observation.testT1 send");
		assertThat(span.getRemoteServiceName()).startsWith("Apache Kafka: ");
		await().until(() -> spans.peekFirst().getTags().size() == 3);
		span = spans.poll();
		assertThat(span.getTags())
				.containsAllEntriesOf(
						Map.of("spring.kafka.listener.id", "obs1-0", "foo", "some foo value", "bar", "some bar value"));
		assertThat(span.getName()).isEqualTo("observation.testT1 receive");
		assertThat(span.getRemoteServiceName()).startsWith("Apache Kafka: ");
		await().until(() -> spans.peekFirst().getTags().size() == 1);
		span = spans.poll();
		assertThat(span.getTags()).containsEntry("spring.kafka.template.name", "template");
		assertThat(span.getName()).isEqualTo("observation.testT2 send");
		await().until(() -> spans.peekFirst().getTags().size() == 3);
		span = spans.poll();
		assertThat(span.getTags())
				.containsAllEntriesOf(
						Map.of("spring.kafka.listener.id", "obs2-0", "foo", "some foo value", "bar", "some bar value"));
		assertThat(span.getName()).isEqualTo("observation.testT2 receive");
		template.setObservationConvention(new DefaultKafkaTemplateObservationConvention() {

			@Override
			public KeyValues getLowCardinalityKeyValues(KafkaRecordSenderContext context) {
				return super.getLowCardinalityKeyValues(context).and("foo", "bar");
			}

		});
		rler.getListenerContainer("obs1").getContainerProperties().setObservationConvention(
				new DefaultKafkaListenerObservationConvention() {

					@Override
					public KeyValues getLowCardinalityKeyValues(KafkaRecordReceiverContext context) {
						return super.getLowCardinalityKeyValues(context).and("baz", "qux");
					}

				});
		rler.getListenerContainer("obs1").stop();
		rler.getListenerContainer("obs1").start();
		template.send("observation.testT1", "test").get(10, TimeUnit.SECONDS);
		assertThat(listener.latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.record).isNotNull();
		headers = listener.record.headers();
		assertThat(headers.lastHeader("foo")).extracting(hdr -> hdr.value()).isEqualTo("some foo value".getBytes());
		assertThat(headers.lastHeader("bar")).extracting(hdr -> hdr.value()).isEqualTo("some bar value".getBytes());
		assertThat(spans).hasSize(4);
		span = spans.poll();
		assertThat(span.getTags()).containsEntry("spring.kafka.template.name", "template");
		assertThat(span.getTags()).containsEntry("foo", "bar");
		assertThat(span.getName()).isEqualTo("observation.testT1 send");
		await().until(() -> spans.peekFirst().getTags().size() == 4);
		span = spans.poll();
		assertThat(span.getTags())
				.containsAllEntriesOf(Map.of("spring.kafka.listener.id", "obs1-0", "foo", "some foo value", "bar",
						"some bar value", "baz", "qux"));
		assertThat(span.getName()).isEqualTo("observation.testT1 receive");
		await().until(() -> spans.peekFirst().getTags().size() == 2);
		span = spans.poll();
		assertThat(span.getTags()).containsEntry("spring.kafka.template.name", "template");
		assertThat(span.getTags()).containsEntry("foo", "bar");
		assertThat(span.getName()).isEqualTo("observation.testT2 send");
		await().until(() -> spans.peekFirst().getTags().size() == 3);
		span = spans.poll();
		assertThat(span.getTags())
				.containsAllEntriesOf(
						Map.of("spring.kafka.listener.id", "obs2-0", "foo", "some foo value", "bar", "some bar value"));
		assertThat(span.getTags()).doesNotContainEntry("baz", "qux");
		assertThat(span.getName()).isEqualTo("observation.testT2 receive");
		MeterRegistryAssert.assertThat(meterRegistry)
				.hasTimerWithNameAndTags("spring.kafka.template",
						KeyValues.of("spring.kafka.template.name", "template"))
				.hasTimerWithNameAndTags("spring.kafka.template",
						KeyValues.of("spring.kafka.template.name", "template", "foo", "bar"))
				.hasTimerWithNameAndTags("spring.kafka.listener", KeyValues.of("spring.kafka.listener.id", "obs1-0"))
				.hasTimerWithNameAndTags("spring.kafka.listener",
						KeyValues.of("spring.kafka.listener.id", "obs1-0", "baz", "qux"))
				.hasTimerWithNameAndTags("spring.kafka.listener", KeyValues.of("spring.kafka.listener.id", "obs2-0"));
		assertThat(admin.getConfigurationProperties())
				.containsEntry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
		// producer factory broker different to admin
		KafkaAdmin pAdmin = KafkaTestUtils.getPropertyValue(template, "kafkaAdmin", KafkaAdmin.class);
		assertThat(pAdmin.getOperationTimeout()).isEqualTo(admin.getOperationTimeout());
		assertThat(pAdmin.getConfigurationProperties())
				.containsEntry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
						broker.getBrokersAsString() + "," + broker.getBrokersAsString());
		// custom admin
		assertThat(customTemplate.getKafkaAdmin()).isSameAs(config.mockAdmin);

		// consumer factory broker different to admin
		Object container = KafkaTestUtils
				.getPropertyValue(endpointRegistry.getListenerContainer("obs1"), "containers", List.class).get(0);
		KafkaAdmin cAdmin = KafkaTestUtils.getPropertyValue(container, "listenerConsumer.kafkaAdmin", KafkaAdmin.class);
		assertThat(cAdmin.getOperationTimeout()).isEqualTo(admin.getOperationTimeout());
		assertThat(cAdmin.getConfigurationProperties())
				.containsEntry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
						broker.getBrokersAsString() + "," + broker.getBrokersAsString() + ","
								+ broker.getBrokersAsString());
		// broker override in annotation
		container = KafkaTestUtils
				.getPropertyValue(endpointRegistry.getListenerContainer("obs2"), "containers", List.class).get(0);
		cAdmin = KafkaTestUtils.getPropertyValue(container, "listenerConsumer.kafkaAdmin", KafkaAdmin.class);
		assertThat(cAdmin.getOperationTimeout()).isEqualTo(admin.getOperationTimeout());
		assertThat(cAdmin.getConfigurationProperties())
				.containsEntry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
		// custom admin
		container = KafkaTestUtils
				.getPropertyValue(endpointRegistry.getListenerContainer("obs3"), "containers", List.class).get(0);
		cAdmin = KafkaTestUtils.getPropertyValue(container, "listenerConsumer.kafkaAdmin", KafkaAdmin.class);
		assertThat(cAdmin).isSameAs(config.mockAdmin);

		assertThatExceptionOfType(KafkaException.class)
				.isThrownBy(() -> template.send("wrong%Topic", "data"))
				.withCauseExactlyInstanceOf(InvalidTopicException.class);

		MeterRegistryAssert.assertThat(meterRegistry)
				.hasTimerWithNameAndTags("spring.kafka.template", KeyValues.of("error", "InvalidTopicException"))
				.doesNotHaveMeterWithNameAndTags("spring.kafka.template", KeyValues.of("error", "KafkaException"));
	}

	@Configuration
	@EnableKafka
	public static class Config {

		KafkaAdmin mockAdmin = mock(KafkaAdmin.class);

		@Bean
		KafkaAdmin admin(EmbeddedKafkaBroker broker) {
			KafkaAdmin admin = new KafkaAdmin(
					Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()));
			admin.setOperationTimeout(42);
			return admin;
		}

		@Bean
		ProducerFactory<Integer, String> producerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
			producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString() + ","
					+ broker.getBrokersAsString());
			return new DefaultKafkaProducerFactory<>(producerProps);
		}

		@Bean
		ConsumerFactory<Integer, String> consumerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("obs", "false", broker);
			consumerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString() + ","
					+ broker.getBrokersAsString() + "," + broker.getBrokersAsString());
			return new DefaultKafkaConsumerFactory<>(consumerProps);
		}

		@Bean
		@Primary
		KafkaTemplate<Integer, String> template(ProducerFactory<Integer, String> pf) {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
			template.setObservationEnabled(true);
			return template;
		}

		@Bean
		KafkaTemplate<Integer, String> customTemplate(ProducerFactory<Integer, String> pf) {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
			template.setObservationEnabled(true);
			template.setKafkaAdmin(this.mockAdmin);
			return template;
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
				ConsumerFactory<Integer, String> cf) {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			factory.getContainerProperties().setObservationEnabled(true);
			factory.setContainerCustomizer(container -> {
				if (container.getListenerId().equals("obs3")) {
					((AbstractMessageListenerContainer<Integer, String>) container).setKafkaAdmin(this.mockAdmin);
				}
			});
			return factory;
		}

		@Bean
		SimpleTracer simpleTracer() {
			return new SimpleTracer();
		}

		@Bean
		MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean
		ObservationRegistry observationRegistry(Tracer tracer, Propagator propagator, MeterRegistry meterRegistry) {
			TestObservationRegistry observationRegistry = TestObservationRegistry.create();
			observationRegistry.observationConfig().observationHandler(
					// Composite will pick the first matching handler
					new ObservationHandler.FirstMatchingCompositeObservationHandler(
							// This is responsible for creating a child span on the sender side
							new PropagatingSenderTracingObservationHandler<>(tracer, propagator),
							// This is responsible for creating a span on the receiver side
							new PropagatingReceiverTracingObservationHandler<>(tracer, propagator),
							// This is responsible for creating a default span
							new DefaultTracingObservationHandler(tracer)))
					.observationHandler(new DefaultMeterObservationHandler(meterRegistry));
			return observationRegistry;
		}

		@Bean
		Propagator propagator(Tracer tracer) {
			return new Propagator() {

				// List of headers required for tracing propagation
				@Override
				public List<String> fields() {
					return Arrays.asList("foo", "bar");
				}

				// This is called on the producer side when the message is being sent
				// Normally we would pass information from tracing context - for tests we don't need to
				@Override
				public <C> void inject(TraceContext context, @Nullable C carrier, Setter<C> setter) {
					setter.set(carrier, "foo", "some foo value");
					setter.set(carrier, "bar", "some bar value");
				}

				// This is called on the consumer side when the message is consumed
				// Normally we would use tools like Extractor from tracing but for tests we are just manually creating a span
				@Override
				public <C> Span.Builder extract(C carrier, Getter<C> getter) {
					String foo = getter.get(carrier, "foo");
					String bar = getter.get(carrier, "bar");
					return tracer.spanBuilder().tag("foo", foo).tag("bar", bar);
				}
			};
		}

		@Bean
		Listener listener(KafkaTemplate<Integer, String> template) {
			return new Listener(template);
		}

	}

	public static class Listener {

		private final KafkaTemplate<Integer, String> template;

		final CountDownLatch latch1 = new CountDownLatch(1);

		final CountDownLatch latch2 = new CountDownLatch(2);

		volatile ConsumerRecord<?, ?> record;

		public Listener(KafkaTemplate<Integer, String> template) {
			this.template = template;
		}

		@KafkaListener(id = "obs1", topics = "observation.testT1")
		void listen1(ConsumerRecord<Integer, String> in) {
			this.template.send("observation.testT2", in.value());
		}

		@KafkaListener(id = "obs2", topics = "observation.testT2",
				properties = ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + ":" + "#{@embeddedKafka.brokersAsString}")
		void listen2(ConsumerRecord<?, ?> in) {
			this.record = in;
			this.latch1.countDown();
			this.latch2.countDown();
		}

		@KafkaListener(id = "obs3", topics = "observation.testT3")
		void listen3(ConsumerRecord<Integer, String> in) {
		}

	}

}
