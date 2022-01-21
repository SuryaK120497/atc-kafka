package com.mit.blob;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;  
  
public class KafkaProducer1 {  
    public static void main(String[] args) {   
    	String bootstrapServers="127.0.0.1:9093";    
        String topic="atcsample";
    	
        //Creating consumer properties  
        Properties properties=new Properties();  
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());  
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
       
        try (  
		KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties)) {
			ProducerRecord<String,String> recordval=new ProducerRecord<String,String>(topic," transformed");
			producer.send(recordval);
	
		}
  
        }  
    }  
