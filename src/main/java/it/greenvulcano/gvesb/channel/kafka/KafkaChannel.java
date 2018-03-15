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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import it.greenvulcano.configuration.XMLConfig;
import it.greenvulcano.configuration.XMLConfigException;
import it.greenvulcano.gvesb.core.config.GreenVulcanoConfig;
import it.greenvulcano.util.metadata.PropertiesHandler;
import it.greenvulcano.util.xpath.XPathFinder;

public class KafkaChannel {
	public static final String HEADER_TAG = "KAKFA_HEADER_";
	
	private final static Logger LOG = LoggerFactory.getLogger(KafkaChannel.class);
	
	private final static AtomicBoolean running = new AtomicBoolean(false);
	private final static ConcurrentMap<String, Producer<String,byte[]>> producers = new ConcurrentHashMap<>();
	private final static ConcurrentMap<String, KafkaForward> consumers = new ConcurrentHashMap<>();
	private static ExecutorService executorService = null;
			
	static void setUp() {
	
		if (running.compareAndSet(false, true)) {			
				
			LOG.debug("Inizialiting GV ESB Kafka plugin module");
			
			try {
				NodeList kafkaChannelList = XMLConfig.getNodeList(GreenVulcanoConfig.getSystemsConfigFileName(),"//Channel[@type='KafkaAdapter' and @enabled='true']");
			
				LOG.debug("Enabled KafkaAdapter channels found: "+kafkaChannelList.getLength());
				
				for (int i = 0; i<kafkaChannelList.getLength(); i++) {
					Node kafkaChannel = kafkaChannelList.item(i);
					
					NodeList kafkaProducersList = XMLConfig.getNodeList(kafkaChannel,"./kafka-publish-call");
					IntStream.range(0, kafkaProducersList.getLength())
			                 .mapToObj(kafkaProducersList::item)		         
			                 .forEach(KafkaChannel::buildProducer);
				}
				
				
				NodeList kafkaConsumers = XMLConfig.getNodeList(GreenVulcanoConfig.getSystemsConfigFileName(),"//Channel[@type='KafkaAdapter' and @enabled='true']/kafka-subscription-listener");
			
				if (kafkaConsumers.getLength()>0) {
					
					IntStream.range(0, kafkaConsumers.getLength())
	                      .mapToObj(kafkaConsumers::item)		         
	                      .forEach(KafkaChannel::buildConsumer);				
					
				} 
				
				if (consumers.size()>0) {
					
					LOG.debug("Configuring KafkaForward thread-pool of size "+consumers.size());
					
					executorService = Executors.newFixedThreadPool(consumers.size());
					consumers.values().stream().forEach(executorService::submit);
					
				}
				
			} catch (XMLConfigException e) {
				LOG.error("Error reading configuration", e);
			}
		} else {
			LOG.debug("GV ESB Kafka plugin module already inizialized");
		}
	}
	
	static void tearDown() {
		if (running.compareAndSet(true, false)) {
			LOG.debug("Finalizing GV ESB Kafka plugin module");
			producers.values().stream().forEach(Producer::close);
			producers.clear();
			
			consumers.values().stream().forEach(KafkaForward::stop);
			consumers.clear();
					
			
			try {
				if (!executorService.awaitTermination(16, TimeUnit.SECONDS)) {
					executorService.shutdown();
				}
			} catch (InterruptedException e) {
				LOG.error("Error terminating consumers task", e);
				executorService.shutdownNow();
			}
			
			executorService = null;
		}
	}	
	private static void buildProducer(Node producerNode) {
		
		try {
   		 
			 String channel = XMLConfig.get(producerNode.getParentNode(), "@id-channel");
			 String system = XMLConfig.get(producerNode.getParentNode().getParentNode(), "@id-system");
			 			
			 String endpoint = XMLConfig.get(producerNode.getParentNode(), "@endpoint");
	   		 String config =  PropertiesHandler.expand(XMLConfig.get(producerNode, "@config"));
	   		 String name = XMLConfig.get(producerNode, "@name");
	   			   		 
	   		 LOG.debug(String.format("Configuring producer %s in channel %s - system %s", name, channel, system));
	   		 
	   		 Properties props = new Properties();
	   		 if (config!=null) {
	   			 LOG.debug(String.format("Reading config properties from %s for producer %s in channel %s - system %s", config, name, channel, system));
	   			 props.load(Files.newInputStream(Paths.get(config), StandardOpenOption.READ));
	   		 }
	   		 
	   		 props.put("bootstrap.servers", endpoint);
	   		 props.put("key.serializer", StringSerializer.class);
	   		 props.put("value.serializer", ByteArraySerializer.class);
	   		 
	   		 Producer<String, byte[]> producer = new KafkaProducer<>(props);
	   		
	   		 String producerKey = XPathFinder.buildXPath(producerNode);
	   		 
	   		 producers.putIfAbsent(producerKey, producer);
   		 
	   		LOG.info(String.format("Configured producer %s in channel %s - system %s", name, channel, system));
   		 
		 } catch (Exception e) {
			 LOG.error("Error configuring producer", e);
			
		 }  	 
   	 
	}	
	
	private static void buildConsumer(Node consumerNode) {
		
		try {
   		 
			 String channel = XMLConfig.get(consumerNode.getParentNode(), "@id-channel");
			 String system = XMLConfig.get(consumerNode.getParentNode().getParentNode(), "@id-system");
			 			
			 String endpoint = XMLConfig.get(consumerNode.getParentNode(), "@endpoint");
	   		 String config =  PropertiesHandler.expand(XMLConfig.get(consumerNode, "@config"));
	   		 String name = XMLConfig.get(consumerNode, "@name");
	   		 String group = XMLConfig.get(consumerNode, "@group");
	   		 
	   		 String service = XMLConfig.get(consumerNode, "@service");
	   		 String operation = XMLConfig.get(consumerNode, "@operation");
	   			   		 
	   		 boolean transactional = Boolean.valueOf(XMLConfig.get(consumerNode, "@transactional", "false"));
	   		 
	   		 LOG.debug(String.format("Configuring producer %s in channel %s - system %s", name, channel, system));
	   		 
	   		 Properties props = new Properties();
	   		 if (config!=null) {
	   			 LOG.debug(String.format("Reading config properties from %s for producer %s in channel %s - system %s", config, name, channel, system));
	   			 props.load(Files.newInputStream(Paths.get(config), StandardOpenOption.READ));
	   		 }
	   		
	   		 
	   		 NodeList topicsNodeList = XMLConfig.getNodeList(consumerNode, "./topic");
	   		 
	   		 Set<String> topics = IntStream.range(0, topicsNodeList.getLength())
	   				                       .mapToObj(topicsNodeList::item)
	   				                       .map(Node::getTextContent)
	   		 	                           .collect(Collectors.toSet());
	   		 
	   		 KafkaForward kafkaForward = new KafkaForward(name, group, endpoint, topics, props, service, operation, transactional);
   		 
	   		 consumers.putIfAbsent(XPathFinder.buildXPath(consumerNode), kafkaForward);
	   		 
	   		 LOG.info(String.format("Configured KafkaForward %s in channel %s - system %s", name, channel, system));
   		 
		 } catch (Exception e) {
			 LOG.error("Error configuring KafkaForward", e);
			
		 }		        	 
   	 
   	 
	}
	
	public static Producer<String, byte[]> getProducer(Node operationNode){		
		String producerKey = XPathFinder.buildXPath(operationNode);		
		return producers.get(producerKey);
	}	
	

}
