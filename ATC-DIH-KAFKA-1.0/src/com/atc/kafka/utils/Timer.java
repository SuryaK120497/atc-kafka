package com.atc.kafka.utils;

import java.util.concurrent.TimeUnit;

public class Timer {

	private long start =01;
	private long end=01;
	public Timer() {
		this.start = System.currentTimeMillis();
		this.end = System.currentTimeMillis();
	}
	
	public void start() {
	this.start =  System.currentTimeMillis();
	}
	
	public long end() {
	
		this.end = System.currentTimeMillis();
		//System.out.println(end-start);
		//System.out.println(TimeUnit.MILLISECONDS.toSeconds(end-start));
		//return TimeUnit.MILLISECONDS.toSeconds(end-start);

		return end-start;
	}
	
	
}
