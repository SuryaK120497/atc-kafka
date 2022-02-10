package com.atc.kafka;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.atc.kafka.utils.KafkaSingletoneProducer;

public class KafkaConsumerFlow {

	public static long destKafkaAggregation;
	public static long transformAggregation;
	public static void main(String[] args) throws XMLStreamException {
		
		String bootstrapServers = "10.121.2.102:9094";
		String grp_id = "multiapp";
		String topic = "supplierxml";
		
		Properties properties = new Properties();
		KafkaProducerFlow producer = new KafkaProducerFlow();
		AzureFileFetch azFetch = new AzureFileFetch();
		
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, grp_id);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		com.atc.kafka.utils.Timer  t1= new com.atc.kafka.utils.Timer();
	
		t1.start();
		@SuppressWarnings("unchecked")
		KafkaConsumer<String, String> consumer = KafkaSingletoneProducer.msgConsumer(properties);
			consumer.subscribe(Arrays.asList(topic));
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				System.out.println("Number of Messages from topic :" + records.count());
				System.out.println("message from partion  :" + records.partitions());
				if (records.count() != 0) {
					transformXML(records, producer);
					azFetch.timeCalculator.put("TransTiming", transformAggregation);
					azFetch.timeCalculator.put("DestProducer " , destKafkaAggregation);
				} else {
					azFetch.timeCalculator.put("kafkaConsumer", t1.end());
				}
				System.out.println("Time taken for each segments :" + azFetch.timeCalculator);
			}
		}
		


	private static void transformXML(ConsumerRecords<String, String> records, KafkaProducerFlow producer)
			throws XMLStreamException {
		
		com.atc.kafka.utils.Timer t2= new com.atc.kafka.utils.Timer();
		
		for (ConsumerRecord<String, String> record : records) {
			t2.start();
			InputStream xmlInput = new ByteArrayInputStream(record.value().getBytes());
			XMLInputFactory inputFactory = XMLInputFactory.newInstance();
			inputFactory.setProperty(XMLInputFactory.IS_COALESCING, true);
			XMLStreamReader reader = inputFactory.createXMLStreamReader(xmlInput);
			String xmlConstruct = "";
			String values = "<";
			String keys = "<Name Path Values>";
			while (reader.hasNext()) {
				switch (reader.next()) {
				case XMLStreamConstants.START_ELEMENT:
					System.out.println("Start " + reader.getName());
					for (int i = 0, count = reader.getAttributeCount(); i < count; i++) {
						System.out.println(reader.getAttributeName(i) + "=" + reader.getAttributeValue(i));

					}
					break;
				case XMLStreamConstants.END_ELEMENT:
					System.out.println("End " + reader.getName());
					break;
				case XMLStreamConstants.CHARACTERS:
				case XMLStreamConstants.SPACE:
					String text = reader.getText();
					if (!text.trim().isEmpty()) {
						System.out.println("text: " + text);
						values = values + " " + text;
					}
					break;
				}
			}
			xmlConstruct = keys + " " + values + " >";
			System.out.println("Final XML :" + xmlConstruct);
            
			sendTranformedXML(xmlConstruct);
			
			transformAggregation =transformAggregation+t2.end();
			
			System.out.println("Transformation aggregation for each iteration :" + transformAggregation);
		}
	}
	public static void sendTranformedXML(String XML) {
		com.atc.kafka.utils.Timer t3= new com.atc.kafka.utils.Timer();
		t3.start();
		System.out.println("Connecting to KAFKA");

		String bootstrapServers = "10.121.2.102:9094";
		String topic = "atctransmulpart";
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		@SuppressWarnings("unchecked")
		KafkaProducer<String, String> producer = KafkaSingletoneProducer.msgProducer(properties);
	
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, XML);
			producer.send(record);
			
			destKafkaAggregation=destKafkaAggregation+t3.end();
			
			System.out.println("kafka producer final aggregation "+ destKafkaAggregation);
		} 
}
