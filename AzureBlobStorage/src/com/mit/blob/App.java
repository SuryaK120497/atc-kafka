package com.mit.blob;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.*;

import com.azure.storage.blob.models.*;
import com.azure.storage.blob.specialized.BlockBlobClient;

import java.io.*;
import java.util.Iterator;
import java.util.Properties;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.commons.io.IOUtils;

public class App {
	public static void main(String[] args) throws IOException {

		
		  System.out.println("Connecting to Azure Blob Storage");
		 
		  BlobServiceClient blobServiceClient = new
		  BlobServiceClientBuilder().connectionString(
		  "DefaultEndpointsProtocol=https;BlobEndpoint=https://sharedmitapsa1.blob.core.windows.net;AccountName=sharedmitapsa1;AccountKey=NWe+Uv3JDHrh5P/+lA5zQ4nD5f2D/Cr0vxroHZUlvCg+ase4PbcyDNgsE7pk6iOzzbFPRaH+BYNdUFHzERRJHQ=="
		  ).buildClient(); 
		
	
		  
		  System.out.println("Connected to Azure Blob : " + blobServiceClient.getAccountName());
		  
		  
		  
		  PagedIterable<BlobContainerItem> a = blobServiceClient.listBlobContainers();
		  
		  Iterator<BlobContainerItem> b= a.iterator();
		  
		  
		  BlobContainerClient containerClient =
		  blobServiceClient.getBlobContainerClient("sharedmitapsa1-dih-dev-nifi");
		  
		  System.out.println("Created Blob Service Client to : sharedmitapsa1-dih-dev-nifi");
		  
		  BlockBlobClient blobClient = containerClient.getBlobClient("java/JPN_8010401090648_Supplier_20220114_020355_280.xml").getBlockBlobClient();
		  
			
		System.out.println("Reading XML files");
          InputStream input =  blobClient.openInputStream();
          InputStreamReader inr = new InputStreamReader(input, "UTF-8");
          
          
          String output = IOUtils.toString(inr);
          System.out.println(output);
          System.out.println("Reading Completed");  
          
          System.out.println("Sending data to KAFKA Topic"); 
		  send(output);
		  
		  while(b.hasNext()) { System.out.println("list :"+b.next().getName()); }
		  
		
		  
		
		  
		  System.out.println(blobServiceClient.listBlobContainers());
		  
		
	}

	private static void send(String output) {
		// TODO Auto-generated method stub
		
		 System.out.println("Connecting to KAFKA"); 
		
	 	//String bootstrapServers="172.18.0.3:9092";
        String topic="atcsample";
        String bootstrapServers="10.121.2.102:9094";
        //Creating consumer properties  
        Properties properties=new Properties();  
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());  
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
       
        try (  
		KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties)) {
			ProducerRecord<String,String> recordval=new ProducerRecord<String,String>(topic,output);
			 System.out.println("sending data to kafka"); 
			 producer.send(recordval);
			 System.out.println("data has been sent"); 
	}
       catch(Exception e) {
    	   System.out.println("Error :" + e);
       }
	}
}
