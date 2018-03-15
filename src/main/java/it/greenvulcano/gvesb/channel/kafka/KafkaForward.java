/*******************************************************************************
 * Copyright (c) 2009, 2016 GreenVulcano ESB Open Source Project.
 * All rights reserved.
 *
 * This file is part of GreenVulcano ESB.
 *
 * GreenVulcano ESB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *  
 * GreenVulcano ESB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *  
 * You should have received a copy of the GNU Lesser General Public License
 * along with GreenVulcano ESB. If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package it.greenvulcano.gvesb.channel.kafka;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import it.greenvulcano.gvesb.buffer.GVBuffer;
import it.greenvulcano.gvesb.buffer.GVException;
import it.greenvulcano.gvesb.buffer.GVPublicException;
import it.greenvulcano.gvesb.core.pool.GreenVulcanoPool;
import it.greenvulcano.gvesb.core.pool.GreenVulcanoPoolException;
import it.greenvulcano.gvesb.core.pool.GreenVulcanoPoolManager;

public class KafkaForward implements Runnable {

	private final static Logger LOG = LoggerFactory.getLogger(KafkaForward.class);

	private final ExecutorService executorService;
	private final Set<String> topics;
	private final String service, operation;
	private final KafkaConsumer<String, byte[]> kafkaConsumer;
	private final AtomicBoolean running = new AtomicBoolean(false);

	private final String name;
	private final boolean transactional;

	private static final long CONSUMER_RATE = 200l;

	KafkaForward(String name, String groupId, String endpoint, Set<String> topics, Properties props, String service,
			String operation, boolean transactional) {

		this.name = name;

		this.executorService = Executors.newWorkStealingPool();
		this.topics = topics;
		this.service = service;
		this.operation = operation;

		this.transactional = transactional;

		props.put("group.id", groupId);
		props.put("enable.auto.commit", "false");
		props.put("bootstrap.servers", endpoint);
		props.put("key.deserializer", StringDeserializer.class);
		props.put("value.deserializer", ByteArrayDeserializer.class);

		this.kafkaConsumer = new KafkaConsumer<>(props);
	}

	public String getService() {
		return service;
	}

	public String getOperation() {
		return operation;
	}

	public boolean isRunning() {
		return running.get();
	}

	public String getName() {
		return name;
	}
	
	void stop() {
		if (running.compareAndSet(true, true)) {
			kafkaConsumer.wakeup();			
		}
	}

	@Override
	public void run() {

		if (running.compareAndSet(false, true)) {

			LOG.debug("KafkaForward[" + name + "] starting consumer loop");

			try {

				kafkaConsumer.subscribe(topics);

				while (running.get()) {

					ConsumerRecords<String, byte[]> messages = kafkaConsumer.poll(CONSUMER_RATE);
					
					/* Stream approach doesn't works because of commit management
					 * 
					 * 
					messages.partitions().stream()
					                     .flatMap(t -> messages.records(t).stream())
					                     .map(this::buildGVBuffer)
					                     .filter(Optional::isPresent)
					                     .map(Optional::get)
					                     .map(GreenVulcanoExecutor::new)
					                     .forEach(executorService::submit);
					*/

					for (TopicPartition topicPartitions : messages.partitions()) {

						/*
						 * Split record by partition allowing commit
						 */
						List<ConsumerRecord<String, byte[]>> partitionMessages = messages.records(topicPartitions);
						OffsetAndMetadata lastSuccessfulOffset = null;

						try {

							for (ConsumerRecord<String, byte[]> message : partitionMessages) {

								GVBuffer gvbuffer = new GVBuffer();

								gvbuffer.setService(service);

								gvbuffer.setProperty("KAFKA_FORWARD_NAME", name);
								gvbuffer.setProperty("KAFKA_FORWARD_TOPIC", message.topic());
								gvbuffer.setProperty("KAFKA_FORWARD_TIMESTAMP", Long.toString(message.timestamp()));
								
								if (Optional.ofNullable(message.key()).isPresent()) {
									gvbuffer.setProperty("KAFKA_FORWARD_KEY", message.key());
								}
																
								gvbuffer.setProperty("KAFKA_FORWARD_PARTITION", Integer.toString(message.partition()));
								gvbuffer.setProperty("KAFKA_FORWARD_OFFSET", Long.toString(message.offset()));

								for (Header header : message.headers()) {
									gvbuffer.setProperty(KafkaChannel.HEADER_TAG.concat(header.key()),
											new String(header.value()));
								}

								gvbuffer.setObject(message.value());

								if (transactional) {

									/*
									 * Blocking execution of GreenVulcano waiting for response to commit
									 */
									GVBuffer result = GreenVulcanoPoolManager.instance()
											.getGreenVulcanoPool("KafkaForward")
											.orElseGet(GreenVulcanoPoolManager::getDefaultGreenVulcanoPool)
											.forward(gvbuffer, operation);

									lastSuccessfulOffset = new OffsetAndMetadata(message.offset() +1,
											                                     result.getId().toString());

								} else {
									/*
									 * Non-Blocking execution of GreenVulcano
									 */
									executorService.submit(new GreenVulcanoExecutor(gvbuffer));
									lastSuccessfulOffset = new OffsetAndMetadata(message.offset() +1);
								}

								kafkaConsumer.commitSync(Collections.singletonMap(topicPartitions, lastSuccessfulOffset));
							}

						} catch (GVPublicException e) {
							LOG.error("KafkaForward[" + name + "] fails to forward message to " + service + "/"
									+ operation, e);

						} catch (GVException e) {
							LOG.error("KafkaForward[" + name + "] fails to build GVBuffer ", e);

						} catch (GreenVulcanoPoolException e) {
							LOG.error("KafkaForward[" + name + "] fails to retrieve a GreenVulcano instance from pool ", e);
						}

					}

				}

			} catch (WakeupException e) {
				LOG.debug("KafkaForward[" + name + "] consumer loop interrupted");

			} catch (Exception e) {
				LOG.error("KafkaForward[" + name + "] fatal error in consumer loop ", e);
			}

			LOG.debug("KafkaForward[" + name + "] exiting consumer loop");
			kafkaConsumer.close();
		} else {
			LOG.debug("KafkaForward[" + name + "] consumer loop already running");
		}
		
		LOG.debug("KafkaForward[" + name + "] exiting from consumer loop");
	}
	
	/*
	private Optional<GVBuffer> buildGVBuffer(ConsumerRecord<String, byte[]> message) {
		try {
			GVBuffer gvbuffer = new GVBuffer();
	
			gvbuffer.setService(service);
	
			gvbuffer.setProperty("KAFKA_FORWARD_NAME", name);
			gvbuffer.setProperty("KAFKA_FORWARD_TOPIC", message.topic());
			gvbuffer.setProperty("KAFKA_FORWARD_TIMESTAMP", Long.toString(message.timestamp()));
			gvbuffer.setProperty("KAFKA_FORWARD_KEY", message.key());
			gvbuffer.setProperty("KAFKA_FORWARD_PARTITION", Integer.toString(message.partition()));
			gvbuffer.setProperty("KAFKA_FORWARD_OFFSET", Long.toString(message.offset()));
	
			for (Header header : message.headers()) {
				gvbuffer.setProperty(KafkaChannel.HEADER_TAG.concat(header.key()),
						new String(header.value()));
			}
	
			gvbuffer.setObject(message.value());
			
			return Optional.of(gvbuffer);
		} catch (GVException e) {
			LOG.error("KafkaForward[" + name + "] fails to build GVBuffer ", e);

		}
		
		return Optional.empty();
		
	}*/

	private class GreenVulcanoExecutor implements Runnable {

		private final GreenVulcanoPool greenVulcano;
		private final GVBuffer inputBuffer;

		GreenVulcanoExecutor(GVBuffer inputBuffer) {
			this.inputBuffer = inputBuffer;
			this.greenVulcano = GreenVulcanoPoolManager.instance().getGreenVulcanoPool("KafkaForward")
					.orElseGet(GreenVulcanoPoolManager::getDefaultGreenVulcanoPool);

		}

		@Override
		public void run() {
			try {
				LOG.debug("KafkaForward[" + name + "] forwarding message to " + inputBuffer.getService() + "/"
						+ getOperation());
				greenVulcano.forward(inputBuffer, getOperation());

			} catch (GVPublicException e) {
				LOG.error("KafkaForward[" + name + "] got error in forward ", e);
			} catch (GreenVulcanoPoolException e) {
				LOG.error("KafkaForward[" + name + "] got error retrieving poolinstance", e);
			}

		}

	}

}