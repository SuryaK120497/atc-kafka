package com.atc.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.atc.kafka.utils.KafkaSingletoneProducer;

public class KafkaProducerFlow {
	com.atc.kafka.utils.Timer t1= new com.atc.kafka.utils.Timer();
	public static long destKafkaAggregation;
	public void sendTranformedXML(String XML) {
		t1.start();
		System.out.println("Connecting to KAFKA");

		String bootstrapServers = "10.121.2.102:9094";
		//String grp_id = "app1";
		String topic = "atctransmulpart";
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		@SuppressWarnings("unchecked")
		KafkaProducer<String, String> producer = KafkaSingletoneProducer.msgProducer(properties);
	
			ProducerRecord<String, String> recordval = new ProducerRecord<String, String>(topic, XML);
			//System.out.println("sending data to kafka Topic :"+ topic);
			producer.send(recordval);
			
			destKafkaAggregation=destKafkaAggregation+t1.end();
			
			System.out.println("data has been sent to kafka Topic :"+ topic);
		} 
	}

