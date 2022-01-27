package com.atc.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.specialized.BlockBlobClient;

public class AzureFileFetch extends TimerTask {

	ArrayList<String> AzureOlderFiles = new ArrayList<>();

	@Override
	public void run() {

		BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(
				"DefaultEndpointsProtocol=https;BlobEndpoint=https://sharedmitapsa1.blob.core.windows.net;AccountName=sharedmitapsa1;AccountKey=NWe+Uv3JDHrh5P/+lA5zQ4nD5f2D/Cr0vxroHZUlvCg+ase4PbcyDNgsE7pk6iOzzbFPRaH+BYNdUFHzERRJHQ==")
				.buildClient();

		BlobContainerClient blobContainerClient = blobServiceClient
				.getBlobContainerClient("sharedmitapsa1-dih-dev-nifi");

		BlobContainerClient blobContainerArchive = blobServiceClient
				.getBlobContainerClient("sharedmitapsa1-dih-dev-nifi/archive");

		System.out.println("Connected to Azure Blob Storage :" + blobServiceClient.getAccountName());

		ArrayList<String> azureNewBlobFiles = new ArrayList<>();
		ArrayList<String> azureNewBlobFilesUrl = new ArrayList<>();

		/*
		 * for (BlobItem blobItem : blobContainerClient.listBlobsByHierarchy("Java/")) {
		 * 
		 * BlockBlobClient blobClient =
		 * blobContainerClient.getBlobClient(blobItem.getName()).getBlockBlobClient();
		 * 
		 * // BlockBlobClient blobClient1 =
		 * blobContainerArchive.getBlobClient(blobItem.getName().replaceAll("Java/",
		 * "")).getBlockBlobClient();
		 * 
		 * // blobClient1.beginCopy(blobClient.getBlobUrl(),null);
		 * System.out.println("done"); //
		 * azureNewBlobFiles.add(blobClient.getBlobName()); }
		 */
		for (BlobItem blobItem : blobContainerClient.listBlobsByHierarchy("Java/")) {

			BlockBlobClient blobClient = blobContainerClient.getBlobClient(blobItem.getName()).getBlockBlobClient();
			azureNewBlobFiles.add(blobClient.getBlobName());
			azureNewBlobFilesUrl.add(blobClient.getBlobUrl());
		}

		System.out.println("List of New Blobs : " + azureNewBlobFiles);

		ArrayList<String> uniqueFiles = new ArrayList<String>(azureNewBlobFiles);

		uniqueFiles.removeAll(AzureOlderFiles);

		System.out.println("List of Delta Blobs : " + uniqueFiles);

		if (uniqueFiles.isEmpty()) {

			System.out.println("No of delta blobs is Zero");
		}

		else {

			AzureOlderFiles = new ArrayList<String>(azureNewBlobFiles);
			System.out.println("Updating Older Blob Details List : " + AzureOlderFiles);

			try {
				System.out.println("Reading Azure Blob Files");
				readAzureBlobs(uniqueFiles, blobContainerClient);
				fileArchive(azureNewBlobFiles, azureNewBlobFilesUrl, blobContainerArchive, blobContainerClient);
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	public void fileArchive(ArrayList<String> azureNewBlobFiles, ArrayList<String> azureNewBlobFilesUrl,
			BlobContainerClient blobContainerArchive, BlobContainerClient blobContainerClient) {

		for (int i = 0; i < azureNewBlobFiles.size(); i++) {

			BlockBlobClient blobClient1 = blobContainerArchive
					.getBlobClient(azureNewBlobFiles.get(i).replaceAll("Java/", "")).getBlockBlobClient();
			if(azureNewBlobFiles.get(i).equals("Java/dummy.txt")) {
				
				i++;
			}
			else {
			BlockBlobClient blobClientdel = blobContainerClient
					.getBlobClient(azureNewBlobFiles.get(i)).getBlockBlobClient();

			blobClient1.beginCopy(azureNewBlobFilesUrl.get(i), null);
			
			blobClientdel.delete();

			}
		}
		System.out.println("Archive process is completed");
	}

	public void readAzureBlobs(ArrayList<String> uniqueFiles, BlobContainerClient blobContainerClient)
			throws IOException {
			
		uniqueFiles.remove("Java/dummy.txt");

		for (String file : uniqueFiles) {

			System.out.println("Reading XML file :" + file);

			BlockBlobClient blobClient = blobContainerClient.getBlobClient(file).getBlockBlobClient();

			InputStream input = blobClient.openInputStream();
			InputStreamReader inr = new InputStreamReader(input, "UTF-8");

			String XmlRecord = IOUtils.toString(inr);

			System.out.println(XmlRecord);

			System.out.println("Reading Completed for the File : " + file);

			System.out.println("Sending a File to KAFKA Topic : " + file);

			kafkaConnection(XmlRecord);

			System.out.println("Flow Completed.......");

		}

	}

	public void kafkaConnection(String XmlRecord) {

		// String bootstrapServers="10.121.2.102:9094";
		// String topicName = "atcpushrawxml";
		String bootstrapServers = "127.0.0.1:9093";
		// String bootstrapServers="10.121.2.102:9094";
		String topicName = "atcrawxml";
		// String bootstrapServers="172.18.0.4:9092";
		// String bootstrapServers = "127.0.0.1:9093";
		// Creating Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties)) {

			System.out.println("Connected to Kafka Topic ");
			ProducerRecord<String, String> recordval = new ProducerRecord<String, String>(topicName, XmlRecord);
			producer.send(recordval);
			System.out.println("Data has been sent to Kafka Topic.........");

		} catch (Exception e) {
			System.out.println("Error while sending data to Kafka Topic :" + e);
		}
	}

	public static void main(String[] args) {

		System.out.println("Invoked Main Method");
		Timer timer = new Timer();
		AzureFileFetch task = new AzureFileFetch();
		timer.scheduleAtFixedRate(task, 10 * 1000, 10 * 1000);

	}

}
