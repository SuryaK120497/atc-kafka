package com.atc.kafka.utils;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

public class AzureBlobIntegration {
	
	private static BlobContainerClient bcc=null;
	private static BlobContainerClient abcc=null;
	private AzureBlobIntegration() {
		
	}
	
	public static BlobContainerClient getBlobContainerClient(){
		if(bcc == null) {
			BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(
					"DefaultEndpointsProtocol=https;BlobEndpoint=https://sharedmitapsa1.blob.core.windows.net;AccountName=sharedmitapsa1;AccountKey=NWe+Uv3JDHrh5P/+lA5zQ4nD5f2D/Cr0vxroHZUlvCg+ase4PbcyDNgsE7pk6iOzzbFPRaH+BYNdUFHzERRJHQ==")
					.buildClient();

			 bcc = blobServiceClient
					.getBlobContainerClient("sharedmitapsa1-dih-dev-nifi");
		}
		
		return bcc;
	}
	
	public static BlobContainerClient getArchieveBlobContainerClient(){
		if(abcc == null) {
			BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(
					"DefaultEndpointsProtocol=https;BlobEndpoint=https://sharedmitapsa1.blob.core.windows.net;AccountName=sharedmitapsa1;AccountKey=NWe+Uv3JDHrh5P/+lA5zQ4nD5f2D/Cr0vxroHZUlvCg+ase4PbcyDNgsE7pk6iOzzbFPRaH+BYNdUFHzERRJHQ==")
					.buildClient();

			abcc = blobServiceClient
					.getBlobContainerClient("sharedmitapsa1-dih-dev-nifi/archive");
		}
		
		return abcc;
	}
	

}
