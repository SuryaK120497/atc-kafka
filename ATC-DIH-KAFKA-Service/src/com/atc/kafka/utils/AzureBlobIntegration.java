package com.atc.kafka.utils;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.file.share.ShareClient;
import com.azure.storage.file.share.ShareClientBuilder;
import com.azure.storage.file.share.ShareDirectoryClient;

public class AzureBlobIntegration {
	
	private static BlobContainerClient bcc=null;
	private static BlobContainerClient abcc=null;
	private static ShareDirectoryClient sfc=null;
	
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
	
	public static ShareDirectoryClient getShareDirectoryClient(){
		String ACCOUNT_NAME = "nonprodmitapsa1";
		String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=nonprodmitapsa1;AccountKey=NdQV5VLRq8JG9fFb92sVU0N2VEuvhIZ6ocB2uwAt+veq5C3omo7bIkN77MZPYwjdWUFr0c9LL2qCUzRBfQ6oPQ==;EndpointSuffix=core.windows.net";
		String shareURL = String.format("https://%s.file.core.windows.net", ACCOUNT_NAME);
		if(sfc == null) {
			ShareClient shareClient = new ShareClientBuilder().endpoint(shareURL).connectionString(CONNECTION_STRING)
					.shareName("nonprodmitapsa1-smb-1-ipm-dev-public-html").buildClient();
			
			 sfc = shareClient.getDirectoryClient("suppliers/CMP/supplier_catalog_exports");
		}
		
		return sfc;
	}
	
	
}
