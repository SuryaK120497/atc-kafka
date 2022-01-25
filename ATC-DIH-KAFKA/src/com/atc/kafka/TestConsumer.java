package com.atc.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import javax.xml.stream.XMLStreamException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class TestConsumer {
	public static void main(String[] args) throws XMLStreamException {
		//String bootstrapServers = "127.0.0.1:9093";
		//String grp_id = "third_app2";
		//String topic = "atcrawxml";
		
		String bootstrapServers = "10.121.2.102:9094";
		String grp_id = "app";
		String topic = "atctransformedxml";
		
		// Creating consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, grp_id);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties)) {
			consumer.subscribe(Arrays.asList(topic));
			while (true) {
				System.out.println("Started Consuming Messages from Topic :" + topic);
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
				System.out.println("Number of Messages from topic :"+ records.count());
				for (ConsumerRecord<String, String> record : records) {
				System.out.println("Value :" + record.value());
				}
				}
			}
		}
}
