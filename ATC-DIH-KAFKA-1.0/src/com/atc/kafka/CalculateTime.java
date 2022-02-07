package com.atc.kafka;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class CalculateTime {
	public static HashMap<String, Long> m = new HashMap<String, Long>();
	public static void main(String[] args) {
		com.atc.kafka.utils.Timer  t1= new com.atc.kafka.utils.Timer();
		t1.start();
		
		String topic = "atcpushrawxml";
		String bootstrapServers = "10.121.2.102:9094";
		
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties)) {
			String input ="sample data";
			ProducerRecord<String, String> recordval = new ProducerRecord<String, String>(topic, input);
			producer.send(recordval);
			try {
				m.put("producer", t1.end());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Time Taken :" + m);
		}catch (Exception e) {
			System.out.println("Error :" + e);
		}
	}

}
