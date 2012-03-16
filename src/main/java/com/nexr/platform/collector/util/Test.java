package com.nexr.platform.collector.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.apache.log4j.Logger;

public class Test {
	static Logger log = Logger.getLogger(Test.class);

	public static void main(String[] args) {
		
		int retries = 0;
		boolean isSuccess = false;
		ExponentialBackoff backoff = new ExponentialBackoff(500, 5);
		
		
		InputStream in = null;
		
		while (!isSuccess) {
			try {
					in = new BufferedInputStream(new FileInputStream(new File("/Users/ryan/check.py")));
					isSuccess = true;
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				long waitTime = backoff.sleepIncrement();
				log.info("attempt " + retries + " failed, backoff ("
            + waitTime + "ms): " + e.getMessage());

				backoff.backoff();
				
				try {
					backoff.waitUntilRetryOk();
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				retries ++;
			}
			
			log.info("isSuccess " + isSuccess);
			if(isSuccess){
				backoff.reset();
				break;
			}
			
			if(backoff.isFailed()){
				//시스템 종
				break;
			}
		}
		log.info("Finish~");
	}

}
