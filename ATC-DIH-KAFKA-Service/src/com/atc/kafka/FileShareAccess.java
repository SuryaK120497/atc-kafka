package com.atc.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.atc.kafka.utils.AzureBlobIntegration;
import com.atc.kafka.utils.KafkaSingletoneProducer;
import com.azure.storage.file.share.ShareDirectoryClient;
import com.azure.storage.file.share.ShareFileClient;
import com.azure.storage.file.share.models.ShareFileItem;

public class FileShareAccess extends TimerTask {

	ArrayList<String> AzureOlderFiles = new ArrayList<>();

	@Override
	public void run() {

		com.atc.kafka.utils.Timer  t1= new com.atc.kafka.utils.Timer();
		t1.start();
		
		ArrayList<String> azureNewFiles = new ArrayList<>();
		
		ShareDirectoryClient directoryClient = AzureBlobIntegration.getShareDirectoryClient();
		
		
		for (ShareFileItem f : directoryClient.listFilesAndDirectories()) {

			azureNewFiles.add(f.getName());
		}
		ArrayList<String> uniqueFiles = new ArrayList<String>(azureNewFiles);

		uniqueFiles.removeAll(AzureOlderFiles);
		
		System.out.println("Over all time taken for blob file extraction:"+t1.end());
		
		timeCalculator.put("AzureFileExtraction", t1.end());

		if (uniqueFiles.isEmpty()) {

			System.out.println("No of delta blobs is Zero");
		} else {

			AzureOlderFiles = new ArrayList<String>(azureNewFiles);
			// System.out.println("Updating Older Blob Details List : " + AzureOlderFiles);

			try {
				readAzureBlobs(uniqueFiles, directoryClient);

				timeCalculator.put("ReadingFiles", fileAggregator);
				timeCalculator.put("KafkaProducer", kafkaAggregation);
				System.out.println("Time taken for each segments : " + timeCalculator);
				
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		
			}

	private static void readAzureBlobs(ArrayList<String> uniqueFiles, ShareDirectoryClient directoryClient) throws IOException {

		
		int i=0;
		com.atc.kafka.utils.Timer t2= new com.atc.kafka.utils.Timer();
		for (String file : uniqueFiles) {
			t2.start();
			ShareFileClient fileClient = directoryClient.getFileClient(file);

		//	System.out.println("File URL :" +fileClient.getFileUrl());
			
			InputStream input = fileClient.openInputStream();
			InputStreamReader inr = new InputStreamReader(input, "UTF-8");

			try {

				String XmlRecord = IOUtils.toString(inr);
				System.out.println("Time taken to read single file :" + t2.end());
				fileAggregator = fileAggregator + t2.end();
				kafkaConnection(XmlRecord);
			} catch (IOException e) {

				e.printStackTrace();
			} finally {
				inr.close();
			}
			
	         i++;
			
			System.out.println("Flow Completed....... with file : " +i);

		}

	}

	private static void kafkaConnection(String xmlRecord) {
	
		String bootstrapServers = "10.121.2.102:9094";
		String topicName = "supplierxml";
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		com.atc.kafka.utils.Timer t3= new com.atc.kafka.utils.Timer();
		t3.start();
		
		@SuppressWarnings("unchecked")
		KafkaProducer<String, String> producer = KafkaSingletoneProducer.msgProducer(properties);

		ProducerRecord<String, String> recordval = new ProducerRecord<String, String>(topicName, xmlRecord);
		producer.send(recordval);
		
		kafkaAggregation = kafkaAggregation +t3.end();
		System.out.println("Kafka aggregation for each iteration :" + kafkaAggregation);
		
		
		
	}

	public HashMap<String, Long> timeCalculator = new HashMap<String, Long>();
	public static long kafkaAggregation;
	public static long fileAggregator;
	public static void main(String[] args) {

		System.out.println("Invoked Main Method");
		Timer timer = new Timer();
		FileShareAccess task = new FileShareAccess();
		timer.scheduleAtFixedRate(task, 1 * 1000, 1 * 1000);

	}

}
