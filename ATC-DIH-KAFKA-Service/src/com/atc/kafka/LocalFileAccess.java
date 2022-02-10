package com.atc.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;

public class LocalFileAccess {

	
	public static void main(String[] args) {
	
		File file = new File("/opt/MDM/public_html/suppliers/CMP/catalog_exports");
		
		String path = file.getAbsolutePath();

		String[] list = file.list();
		
		System.out.println("File Count " + list.length);
		
		for(int i=0;i<list.length;i++) {
			
			String s1= path+"/"+list[i];
			
			System.out.println(s1);
			
			File file1 = new File(s1);
			
        try (InputStream in = new FileInputStream(file1))
        {
            String contents = IOUtils.toString(in, StandardCharsets.UTF_8);
            System.out.println(contents);
        }
        catch (IOException e) {
            e.printStackTrace();
        }   


	}
	}
}