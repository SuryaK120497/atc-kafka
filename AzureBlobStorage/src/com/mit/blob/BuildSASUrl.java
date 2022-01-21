package com.mit.blob;

import java.time.OffsetDateTime;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.sas.AccountSasSignatureValues;
import com.azure.storage.common.sas.SasProtocol;

public class BuildSASUrl {

	public static void main(String[] args) {
		BlobContainerSasPermission blobContainerSasPermission = new BlobContainerSasPermission().setReadPermission(true)
				.setWritePermission(true).setListPermission(true);
		BlobServiceSasSignatureValues builder = new BlobServiceSasSignatureValues(OffsetDateTime.now().plusDays(1),
				blobContainerSasPermission).setProtocol(SasProtocol.HTTPS_ONLY);
	
		
		BlobClient client = new BlobClientBuilder().connectionString("DefaultEndpointsProtocol=https;BlobEndpoint=https://sharedmitapsa1.blob.core.windows.net;AccountName=sharedmitapsa1;AccountKey=NWe+Uv3JDHrh5P/+lA5zQ4nD5f2D/Cr0vxroHZUlvCg+ase4PbcyDNgsE7pk6iOzzbFPRaH+BYNdUFHzERRJHQ==").blobName("java/JPN_8010401090648_Supplier_20220114_020355_280.xml").buildClient();
		String blobContainerName = "sharedmitapsa1-dih-dev-nifi";
		String t =String.format("https://%s.blob.core.windows.net/%s?%s", client.getAccountName(), blobContainerName,
				client.generateSas(builder));
		System.out.println(t);
	}

}
