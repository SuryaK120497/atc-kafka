package com.atc.kafka;

import java.util.Iterator;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.file.share.ShareClient;
import com.azure.storage.file.share.ShareClientBuilder;
import com.azure.storage.file.share.ShareDirectoryClient;
import com.azure.storage.file.share.models.ShareFileItem;
import com.azure.storage.file.share.models.ShareItem;

public class FileShareAccess {

	public static void main(String[] args) {
		
		String ACCOUNT_NAME ="nonprodmitapsa1";
		String SAS_TOKEN = "?sv=2020-08-04&ss=f&srt=sco&sp=rwdlc&se=2022-02-04T12:44:37Z&st=2022-02-04T04:44:37Z&spr=https&sig=x92QJedTSLRty5tEx3f2wuG%2FmV8%2F7281kCZLLs5y%2FuI%3D";
		String shareServiceURL = String.format("https://%s.file.core.windows.net", ACCOUNT_NAME);
	
	//	ShareServiceClient shareServiceClient = new ShareServiceClientBuilder().endpoint(shareServiceURL)
		 //   .sasToken(SAS_TOKEN).buildClient();
		
		
		
		
		String shareURL = String.format("https://%s.file.core.windows.net", ACCOUNT_NAME);
		ShareClient shareClient = new ShareClientBuilder().endpoint(shareURL)
		    .sasToken(SAS_TOKEN).shareName("nonprodmitapsa1-smb-1-ipm-dev-public-html").buildClient();
		
	
		
		System.out.println(shareClient.getAccountName());
		
		ShareDirectoryClient dir =shareClient.getDirectoryClient("suppliers/CMP");
		
		PagedIterable<ShareFileItem> k =dir.listFilesAndDirectories();
		
		Iterator<ShareFileItem> w =k.iterator();
		
		while(w.hasNext()) {
			
			System.out.println("File Name :"+w.next().getName());		
		}
	
	}

}

