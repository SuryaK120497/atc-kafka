package com.atc.kafka;

import java.util.HashMap;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class TestProducer {

	public static HashMap<String, Long> m = new HashMap<String, Long>();
	
public static void main(String[] args) {
		

	//	System.out.println("Connecting to KAFKA");
	com.atc.kafka.utils.Timer  t1= new com.atc.kafka.utils.Timer();
	t1.start();
		//String bootstrapServers = "127.0.0.1:9093";
		//String bootstrapServers="172.18.0.3:9092";
		String topic = "atcsample";
		String bootstrapServers = "10.121.2.102:9094";
		// Creating consumer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		
		
		try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties)) {
			
		//	m.put("Kafka connect", 	t1.end());
			String input = "<Nodes>\r\n"
					+ "   <Node>\r\n"
					+ "      <Name>Supplier</Name>\r\n"
					+ "      <Path>Supplier</Path>\r\n"
					+ "      <Value>6788</Value>\r\n"
					+ "      <Nodes>\r\n"
					+ "         <Node>\r\n"
					+ "               <Name>SupplierPK</Name>\r\n"
					+ "               <Path>Supplier/SupplierPK</Path>\r\n"
					+ "               <Value>S00000021</Value>\r\n"
					+ "         </Node>\r\n"
					+ "         <Node>\r\n"
					+ "               <Name>CountryCode</Name>\r\n"
					+ "               <Path>Supplier/CountryCode</Path>\r\n"
					+ "               <Value>JPN</Value>\r\n"
					+ "         </Node>\r\n"
					+ "		</Nodes>\r\n"
					+ "	</Node>\r\n"
					+ "</Nodes>";
			//byte[] compressed = Snappy.compress(input.getBytes("UTF-8"));
		//	com.atc.kafka.utils.Timer  t2= new com.atc.kafka.utils.Timer();
		//	t2.start();
			ProducerRecord<String, String> recordval = new ProducerRecord<String, String>(topic, input);
			System.out.println("sending data to kafka");
			producer.send(recordval);
			m.put("producer", t1.end());
			System.out.println("data has been sent");
		//	long millisec = t2.end();
			//System.out.println("milli "+ millisec);
			System.out.println("Time taken"+ m);
			
		} catch (Exception e) {
			System.out.println("Error :" + e);
		}
	}
}
