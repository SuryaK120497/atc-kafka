package com.mit.blob;

import java.util.Locale;

import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.specialized.BlockBlobClient;

public class AzureSaS {
	
	public static void main(String[] args) {
		
	//	 String endpoint = String.format(Locale.ROOT,
		      //      "https://sharedmitapsa1.blob.core.windows.net", "sharedmitapsa1");
		    
		    AzureSasCredential sasCredential = new AzureSasCredential(
		            "?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2022-01-21T14:55:29Z&st=2022-01-21T06:55:29Z&spr=https&sig=R0U1o8bU3u17b8mDfFWlO1JgiNEFt6BJkUBVvIGdR2U%3D");
		    BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
		            .endpoint("https://sharedmitapsa1.blob.core.windows.net").credential(sasCredential).buildClient();
		    
		    System.out.println("Started");
		    BlobContainerClient blobContainerClient = blobServiceClient
		            .getBlobContainerClient("sharedmitapsa1-dih-dev-nifi");
		    
		    System.out.println("Started 1");
		    
		    for (BlobItem blobItem : blobContainerClient.listBlobs()) {
		        BlockBlobClient blobClient = blobContainerClient
		                .getBlobClient(blobItem.getName()).getBlockBlobClient();
		        System.out.println(blobClient.getBlobName());
		    }
	}

}
