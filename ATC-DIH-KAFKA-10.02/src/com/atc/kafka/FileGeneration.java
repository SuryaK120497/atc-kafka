package com.atc.kafka;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;

public class FileGeneration {
	public static void main(String[] args) {

		BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString("DefaultEndpointsProtocol=https;BlobEndpoint=https://sharedmitapsa1.blob.core.windows.net;AccountName=sharedmitapsa1;AccountKey=NWe+Uv3JDHrh5P/+lA5zQ4nD5f2D/Cr0vxroHZUlvCg+ase4PbcyDNgsE7pk6iOzzbFPRaH+BYNdUFHzERRJHQ==").buildClient();



		BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient("sharedmitapsa1-dih-dev-nifi");



		PagedIterable<BlobItem> blobs= blobContainerClient.listBlobsByHierarchy("Java/");

		for(int i=0;i<9999;i++) {
			
			for (BlobItem blobItem : blobs) {

				System.out.println("This is the blob name: " + blobItem.getName());
				BlobClient blobClient=blobContainerClient.getBlobClient(blobItem.getName());
				BlobClient destblobclient=blobContainerClient.getBlobClient("testperf/"+blobItem.getName().replaceAll("Java/", "")+"_"+i);
				destblobclient.beginCopy(blobClient.getBlobUrl(),null);
				//BlobClient destblobclient=destcontainer.listBlobsByHierarchy("testperf")

			}
		}
		
}
}
