package com.mit.blob;


import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.specialized.BlockBlobClient;

public class AzureBlobTimer extends TimerTask {
	 
	 ArrayList<String> b1 = new ArrayList<>();
    @Override
    public void run() {
    	
        System.out.println("Fixed rate timer task executed ::  " + Thread.currentThread().getName());
        AzureSasCredential sasCredential = new AzureSasCredential(
	            "?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2022-01-21T14:55:29Z&st=2022-01-21T06:55:29Z&spr=https&sig=R0U1o8bU3u17b8mDfFWlO1JgiNEFt6BJkUBVvIGdR2U%3D");
	    BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
	            .endpoint("https://sharedmitapsa1.blob.core.windows.net").credential(sasCredential).buildClient();
	    
	    BlobContainerClient blobContainerClient = blobServiceClient
	            .getBlobContainerClient("sharedmitapsa1-dih-dev-nifi");
	    ArrayList<String> a1 = new ArrayList<>();
	    for (BlobItem blobItem : blobContainerClient.listBlobs()) {
	        BlockBlobClient blobClient = blobContainerClient
	                .getBlobClient(blobItem.getName()).getBlockBlobClient();
	        a1.add(blobClient.getBlobName());
	    }
	    
	    a1.removeAll(b1);
	    
	    System.out.println("remove" + a1);
	    
	    b1.addAll(a1);
	    
	   System.out.println(b1);
	  
	    
    }
     


	public static void main(String[] args) {
                 
        Timer timer = new Timer(); // Instantiating a timer object
         
        AzureBlobTimer task1 = new AzureBlobTimer(); // Creating a FixedRateSchedulingUsingTimerTask
        timer.scheduleAtFixedRate(task1, 5 * 1000, 5 * 1000); // Scheduling it to be executed with fixed rate at every two seconds
         
     
    }
}
