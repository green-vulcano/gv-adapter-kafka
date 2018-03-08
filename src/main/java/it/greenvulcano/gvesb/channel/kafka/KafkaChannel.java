package it.greenvulcano.gvesb.channel.kafka;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
	
	private final static Logger LOG = LoggerFactory.getLogger(KafkaChannel.class);
	
	private final static ConcurrentMap<String, Producer<String,byte[]>> producers = new ConcurrentHashMap<>();
		
	static void setUp() {
		
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
		
		} catch (XMLConfigException e) {
			LOG.error("Error reading configuration", e);
		}
	}
	
	static void tearDown() {
		LOG.debug("Finalizing GV ESB Kafka plugin module");
		producers.values().stream().forEach(Producer::close);
		producers.clear();
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
	
	public static Producer<String, byte[]> getProducer(Node operationNode){		
		String producerKey = XPathFinder.buildXPath(operationNode);		
		return producers.get(producerKey);
	}	
	

}
