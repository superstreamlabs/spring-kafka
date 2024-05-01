/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.kafka.support.serializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;

/**
 * @author Gary Russell
 * @author Wang Zhiyang
 * @author Soby Chacko
 * @author Artem Bilan
 *
 * @since 2.3
 */
class RetryingDeserializerTests {

	@Test
	void basicRetryingDeserializer() {
		AtomicInteger n = new AtomicInteger();
		Deserializer<String> delegate =
				(topic, data) -> {
					if (n.incrementAndGet() < 3) {
						throw new RuntimeException();
					}
					return new String(data);
				};

		RetryingDeserializer<String> rdes = new RetryingDeserializer<>(delegate, new RetryTemplate());
		assertThat(rdes.deserialize("foo", "bar".getBytes())).isEqualTo("bar");
		assertThat(n.get()).isEqualTo(3);
		n.set(0);
		assertThat(rdes.deserialize("foo", new RecordHeaders(), "bar".getBytes())).isEqualTo("bar");
		assertThat(n.get()).isEqualTo(3);
	}

	@Test
	void retryingDeserializerWithRecoveryCallback() throws Exception {
		RetryingDeserializer<String> rdes =
				new RetryingDeserializer<>(
						(s, b) -> {
							throw new RuntimeException();
						}, new RetryTemplate());
		RecoveryCallback<String> recoveryCallback = mock();
		rdes.setRecoveryCallback(recoveryCallback);
		rdes.deserialize("my-topic", "my-data".getBytes());
		verify(recoveryCallback).recover(any(RetryContext.class));
	}

}
