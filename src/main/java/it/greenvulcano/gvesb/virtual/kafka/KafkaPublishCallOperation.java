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
package it.greenvulcano.gvesb.virtual.kafka;

import it.greenvulcano.configuration.XMLConfig;
import it.greenvulcano.gvesb.buffer.GVBuffer;
import it.greenvulcano.gvesb.channel.kafka.KafkaChannel;
import it.greenvulcano.gvesb.internal.data.GVBufferPropertiesHelper;
import it.greenvulcano.gvesb.virtual.CallException;
import it.greenvulcano.gvesb.virtual.CallOperation;
import it.greenvulcano.gvesb.virtual.ConnectionException;
import it.greenvulcano.gvesb.virtual.InitializationException;
import it.greenvulcano.gvesb.virtual.InvalidDataException;
import it.greenvulcano.gvesb.virtual.OperationKey;
import it.greenvulcano.util.metadata.PropertiesHandler;
import it.greenvulcano.util.metadata.PropertiesHandlerException;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.w3c.dom.Node;


/**
 * 
 * @version 4.0 
 * @author GreenVulcano Developer Team
 */
public class KafkaPublishCallOperation implements CallOperation {
    
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(KafkaPublishCallOperation.class);    
    private static final String HEADER_TAG = "KAKFA_HEADER_";
    
    private OperationKey key = null;
    
    private Producer<String, byte[]> producer;
       
    private String topic = null;
    private String partitionKey = null;
    private String partitionNumber = null;
    
    private boolean logDump = false;
    private boolean waitResult = false;
    
    @Override
    public void init(Node node) throws InitializationException  {
       
        try {
        	producer = Optional.ofNullable(KafkaChannel.getProducer(node))
        			           .orElseThrow(() -> new IllegalArgumentException("Failed to retrieve Producer instance"));
        	
        	topic = Optional.ofNullable(XMLConfig.get(node, "@topic"))
        			        .orElseThrow(() -> new IllegalArgumentException("Missing configuration entry: topic"));
        	
        	logDump = Boolean.valueOf(XMLConfig.get(node, "@log", "false" ));
        	waitResult = Boolean.valueOf(XMLConfig.get(node, "@sync", "false" ));
        	        	       	
            partitionKey = XMLConfig.get(node, "@key");
            partitionNumber = XMLConfig.get(node, "@partition");           	         	
              
        	     
        } catch (Exception exc) {
            throw new InitializationException("GV_INIT_SERVICE_ERROR", new String[][]{{"message", exc.getMessage()}},
                    exc);
        }


    }
           

    @Override
    public GVBuffer perform(GVBuffer gvBuffer) throws ConnectionException, CallException, InvalidDataException {
       
        try {
        	
        	  Map<String, Object> params  = GVBufferPropertiesHelper.getPropertiesMapSO(gvBuffer, true);
        	
        	  String actualTopic = expand(topic, params); 
        	        	  
        	  String key = expand(partitionKey, params);
        	  Integer partition = partitionNumber!=null ? Integer.valueOf(expand(partitionNumber, params)): null;
        	
        	  byte[] message = new byte[] {};
        	  if (gvBuffer.getObject() != null) {        		 
        		  
        		  if (gvBuffer.getObject() instanceof byte[]) {
        			  message = (byte[]) gvBuffer.getObject();
        		  } else if (gvBuffer.getObject() instanceof String) {
        			  
        			  String charset = Optional.ofNullable(gvBuffer.getProperty("OBJECT_ENCODING")).orElse("UTF-8");        			  
        			  message = gvBuffer.getObject().toString().getBytes(charset);
        			  
        		  } else {
        		         		  
	        		  try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
	        			  
	        			  ObjectOutput objectOutput = new ObjectOutputStream(outputStream);
	        			  objectOutput.writeObject(gvBuffer.getObject());
	        			  objectOutput.flush();
	        			  
	        			  message = outputStream.toByteArray(); 
	        		  }
        		  }
        	  }
        	  
        	  logger.debug("Sending message on topic "+actualTopic
        			       + (partition!=null? " partition "+partition: "")
        			       + (key!=null? " key "+key: ""));
        	  ProducerRecord<String, byte[]> record = new ProducerRecord<>(actualTopic, partition, key,  message);
        	  
        	  
        	  gvBuffer.getPropertyNamesSet()
        	          .stream()
        	          .filter(k->k.matches("^"+HEADER_TAG+".*"))
        	          .map(k->k.substring(HEADER_TAG.length()))
        	          .forEach(k-> record.headers().add(k, gvBuffer.getProperty(HEADER_TAG.concat(k)).getBytes(StandardCharsets.UTF_8)));       	  
        	  
        	  
        	  if (logDump) {
        		  logger.debug("GV ESB Kafka plugin module - sending record: "+record.toString());  
        	  }
        	          	  
        	  Future<RecordMetadata> sendResult = producer.send(record);
        	  
        	  if (waitResult) {
        		 RecordMetadata result = sendResult.get();
        		 
        		 gvBuffer.setProperty("KAFKA_RECORD_OFFSET", String.valueOf(result.offset()));
        		 gvBuffer.setProperty("KAFKA_RECORD_TIMESTAMP", String.valueOf(result.timestamp()));
        		 
        	  }    	  
	          
           
        } catch (Exception exc) {
            throw new CallException("GV_CALL_SERVICE_ERROR", new String[][]{{"service", gvBuffer.getService()},
                    {"system", gvBuffer.getSystem()}, {"tid", gvBuffer.getId().toString()},
                    {"message", exc.getMessage()}}, exc);
        }
        
        return gvBuffer;
    }    
   
    @Override
    public void cleanUp(){
        // do nothing
    }
    
    @Override
    public void destroy(){
        // do nothing
    }

    @Override
    public String getServiceAlias(GVBuffer gvBuffer){
        return gvBuffer.getService();
    }

    @Override
    public void setKey(OperationKey key){
        this.key = key;
    }
    
    @Override
    public OperationKey getKey(){
        return key;
    }
    
    private String expand(String entry, Map<String, Object> params) throws PropertiesHandlerException {
    	String actualEntry = PropertiesHandler.isExpanded(entry)? entry :  PropertiesHandler.expand(entry, params);
    	
  	    if (!PropertiesHandler.isExpanded(actualEntry)) {  	    	
  	    	throw new IllegalArgumentException("Failed to expand value: "+entry);
  	    } else if ("null".equalsIgnoreCase(actualEntry)) {
  	    	return null;
  	    }
  	    
  	    return actualEntry;
    }
}
