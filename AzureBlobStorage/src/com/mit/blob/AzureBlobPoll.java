package com.mit.blob;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.changefeed.BlobChangefeedClient;
import com.azure.storage.blob.changefeed.BlobChangefeedClientBuilder;

public class AzureBlobPoll {

	public static void main(String[] args) {
		
		  System.out.println("Connecting to Azure Blob Storage");
			 
		  BlobServiceClient blobServiceClient = new
		  BlobServiceClientBuilder().connectionString(
		  "DefaultEndpointsProtocol=https;BlobEndpoint=https://sharedmitapsa1.blob.core.windows.net;AccountName=sharedmitapsa1;AccountKey=NWe+Uv3JDHrh5P/+lA5zQ4nD5f2D/Cr0vxroHZUlvCg+ase4PbcyDNgsE7pk6iOzzbFPRaH+BYNdUFHzERRJHQ=="
		  ).buildClient(); 
		  
		  BlobChangefeedClient client = new BlobChangefeedClientBuilder(blobServiceClient).buildClient();
		  
		  client.getEvents().forEach(event ->
		    System.out.printf("Topic: %s, Subject: %s%n", event.getTopic(), event.getSubject()));
		  
	}
}
