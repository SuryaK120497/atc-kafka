package com.atc.kafka.utils;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class KafkaSingletoneProducer {
	// private static Map<String, KafkaProducer<?, ?>> mapProducer = new HashMap<String, KafkaProducer<?, ?>>();
	 private static KafkaProducer<?, ?> producer;
	 private static KafkaConsumer<?, ?> consumer;
	 
	 private KafkaSingletoneProducer() {
		 
	 }
	 
	 public static KafkaProducer msgProducer(Properties properties) {
		 if(producer == null) {
		 producer = new KafkaProducer<String,String>(properties); 
		 }
		  return producer;
	
		 }
	 
	 public static KafkaConsumer msgConsumer(Properties properties) {
		 if(consumer == null) {
			 consumer = new KafkaConsumer<String,String>(properties); 
		 }
		  return consumer;
	
		 }

}
