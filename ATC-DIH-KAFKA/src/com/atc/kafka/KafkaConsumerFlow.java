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
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerFlow {

	public static void main(String[] args) throws XMLStreamException {
		String bootstrapServers = "127.0.0.1:9093";
		String grp_id = "app1";
	//	String bootstrapServers = "10.121.2.102:9094";
		//String topic = "atcpushrawxml";
		String topic = "atcrawxml";
		// Creating consumer properties";
		Properties properties = new Properties();
		KafkaProducerFlow producer = new KafkaProducerFlow();
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
				if(records.count()!=0) {
					System.out.println("Calling XML Transformer.........");
					transformXML(records,producer);
				}
				else {
					System.out.println("No records to Consume from kafka topic");
				}
				
			}
		}

	}

	private static void transformXML(ConsumerRecords<String, String> records,KafkaProducerFlow producer) throws XMLStreamException {

		for (ConsumerRecord<String, String> record : records) {
			
			InputStream xmlInput = new ByteArrayInputStream(record.value().getBytes());
			XMLInputFactory inputFactory = XMLInputFactory.newInstance();
			inputFactory.setProperty(XMLInputFactory.IS_COALESCING, true);
			XMLStreamReader reader = inputFactory.createXMLStreamReader(xmlInput);
			String xmlConstruct="";
			String values ="<";
			String keys="<Name Path Values>";
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
				values=values+" " +text;
				}
				break;
				}
				}
			xmlConstruct = keys+" "+values +" >";
			System.out.println("Final XML :"+xmlConstruct);
			
			producer.sendTranformedXML(xmlConstruct);
		}

	}
}
