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
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.specialized.BlockBlobClient;


public class AzureFileFetch extends TimerTask {

	ArrayList<String> AzureOlderFiles = new ArrayList<>();

	@Override
	public void run() {

		com.atc.kafka.utils.Timer  t1= new com.atc.kafka.utils.Timer();
		t1.start();
		
		BlobContainerClient blobContainerClient = AzureBlobIntegration.getBlobContainerClient();
		ArrayList<String> azureNewBlobFiles = new ArrayList<>();
    
		for (BlobItem blobItem : blobContainerClient.listBlobsByHierarchy("testperf/")) {
			
			BlockBlobClient blobClient = blobContainerClient.getBlobClient(blobItem.getName()).getBlockBlobClient();
			
			azureNewBlobFiles.add(blobClient.getBlobName());
		}


		ArrayList<String> uniqueFiles = new ArrayList<String>(azureNewBlobFiles);

		uniqueFiles.removeAll(AzureOlderFiles);
		
		System.out.println("Over all time taken for blob file extraction:"+t1.end());
		
		timeCalculator.put("AzureFileExtraction", t1.end());

		if (uniqueFiles.isEmpty()) {

			System.out.println("No of delta blobs is Zero");
		}

		else {

			AzureOlderFiles = new ArrayList<String>(azureNewBlobFiles);
		//	System.out.println("Updating Older Blob Details List : " + AzureOlderFiles);

			try {
				readAzureBlobs(uniqueFiles, blobContainerClient);
				timeCalculator.put("ReadingFiles", fileAggregator);
				timeCalculator.put("KafkaProducer", kafkaAggregation);
				System.out.println("Time taken for each segments : " + timeCalculator);
			//	fileArchive(azureNewBlobFiles, azureNewBlobFilesUrl, archiveblobContainerClient, blobContainerClient);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	public void fileArchive(ArrayList<String> azureNewBlobFiles, ArrayList<String> azureNewBlobFilesUrl,
			BlobContainerClient archiveblobContainerClient, BlobContainerClient blobContainerClient) {


		for (int i = 0; i < azureNewBlobFiles.size(); i++) {

			BlockBlobClient blobClient1 = blobContainerClient
					.getBlobClient(azureNewBlobFiles.get(i).replaceAll("Java/", "")).getBlockBlobClient();
			if (azureNewBlobFiles.get(i).equals("Java/dummy.txt")) {

				i++;
			} else {
				BlockBlobClient blobClientdel = blobContainerClient.getBlobClient(azureNewBlobFiles.get(i))
						.getBlockBlobClient();

				blobClient1.beginCopy(azureNewBlobFilesUrl.get(i), null);

				blobClientdel.delete();

			}
		}
	//	System.out.println("Archive process is completed");
	}

	public void readAzureBlobs(ArrayList<String> uniqueFiles, BlobContainerClient blobContainerClient) throws IOException
		 {

		uniqueFiles.remove("Java/dummy.txt");
		int i=0;
		com.atc.kafka.utils.Timer t2= new com.atc.kafka.utils.Timer();
		for (String file : uniqueFiles) {

		//	System.out.println("Reading XML file :" + file);
			
			t2.start();
			BlockBlobClient blobClient = blobContainerClient.getBlobClient(file).getBlockBlobClient();

			InputStream input = blobClient.openInputStream();
			InputStreamReader inr = new InputStreamReader(input, "UTF-8");
			try {
				
				String XmlRecord = IOUtils.toString(inr);
				System.out.println("Time taken to read single file :" + t2.end());
				fileAggregator = fileAggregator + t2.end();
				kafkaConnection(XmlRecord);
				System.out.println("Files aggregation for each iteration :" + fileAggregator);
			} catch (IOException e) {
				
				e.printStackTrace();
			} finally {
				inr.close();
			}

			

			i++;
			
			System.out.println("Flow Completed....... with file : " +i);

		}
		
		
	}

	public void kafkaConnection(String XmlRecord) {

		String bootstrapServers = "10.121.2.102:9094";
		String topicName = "atc10ktest";
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


		com.atc.kafka.utils.Timer t3= new com.atc.kafka.utils.Timer();
		t3.start();
		
		@SuppressWarnings("unchecked")
		
		KafkaProducer<String, String> producer = KafkaSingletoneProducer.msgProducer(properties);

		
		ProducerRecord<String, String> recordval = new ProducerRecord<String, String>(topicName, XmlRecord);
		producer.send(recordval);
		kafkaAggregation = kafkaAggregation +t3.end();
		System.out.println("Kafka aggregation for each iteration :" + kafkaAggregation);
	}

	//private static Logger logger = LoggerFactory.getLogger(AzureFileFetch.class);
	public HashMap<String, Long> timeCalculator = new HashMap<String, Long>();
	public static long kafkaAggregation;
	public static long fileAggregator;
	public static void main(String[] args) {

		System.out.println("Invoked Main Method");
		Timer timer = new Timer();
		AzureFileFetch task = new AzureFileFetch();
		timer.scheduleAtFixedRate(task, 1 * 1000, 1 * 1000);
		
	}

}
