package com.atc.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.atc.kafka.utils.KafkaSingletoneProducer;

public class IPMSourceToKafka extends TimerTask {

	public HashMap<String, Long> timeCalculator = new HashMap<String, Long>();
	public static long kafkaAggregation;
	public static long fileAggregator;
	ArrayList<String> oldFiles = new ArrayList<>();

	@Override
	public void run() {

		com.atc.kafka.utils.Timer t1 = new com.atc.kafka.utils.Timer();
		t1.start();

	 	File file = new File("/opt/MDM/public_html/suppliers/CMP/supplier_catalog_exports");


		String filePath = file.getAbsolutePath();

		String[] FileList = file.list();

		List<String> fileList = Arrays.asList(FileList);

		ArrayList<String> uniqueFiles = new ArrayList<String>(fileList);

		uniqueFiles.removeAll(oldFiles);

		System.out.println("Over all time taken for file extraction:" + t1.end());
		
		timeCalculator.put("AzureFileExtraction", t1.end());

		if (uniqueFiles.isEmpty()) {

			System.out.println("No Delta Files Available");
		} else {
			oldFiles = new ArrayList<String>(fileList);

			try {
				readFiles(uniqueFiles, filePath);

				timeCalculator.put("ReadingFiles", fileAggregator);
				timeCalculator.put("KafkaProducer", kafkaAggregation);
				System.out.println("Time taken for each segments : " + timeCalculator);

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	private void readFiles(ArrayList<String> uniqueFiles, String filePath) throws IOException {

		int i = 0;
		com.atc.kafka.utils.Timer t2 = new com.atc.kafka.utils.Timer();
		for (String file : uniqueFiles) {

			t2.start();

			String filePathGenerator = filePath + "\\" + file;

			File  fileRead= new File(filePathGenerator);

			InputStream inputStream = new FileInputStream(fileRead);

			try {
				String xmlRecord = IOUtils.toString(inputStream, "UTF-8");
				System.out.println("Time taken to read single file :" + t2.end());
				fileAggregator = fileAggregator + t2.end();
				kafkaConnection(xmlRecord);
			} catch (IOException e) {
				e.printStackTrace();
			} finally {

				inputStream.close();
			}

			i++;

			System.out.println("Flow Completed....... with file : " + i);

		}
	}

	private void kafkaConnection(String xmlRecord) {
		String bootstrapServers = "10.121.2.102:9094";
		String topicName = "atc10ktest";
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		com.atc.kafka.utils.Timer t3 = new com.atc.kafka.utils.Timer();
		t3.start();

		@SuppressWarnings("unchecked")

		KafkaProducer<String, String> producer = KafkaSingletoneProducer.msgProducer(properties);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, xmlRecord);
		producer.send(record);
		kafkaAggregation = kafkaAggregation + t3.end();
		System.out.println("Kafka aggregation for each iteration :" + kafkaAggregation);

	}

	public static void main(String[] args) {

		System.out.println("Invoked Main Method");
		Timer timer = new Timer();
		IPMSourceToKafka task = new IPMSourceToKafka();
		timer.scheduleAtFixedRate(task, 1 * 1000, 1 * 1000);

	}

}
