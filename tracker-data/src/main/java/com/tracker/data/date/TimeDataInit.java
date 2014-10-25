package com.tracker.data.date;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class TimeDataInit {
	public static void main(String[] args) throws IOException {
		int index = 1;
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("data/date/time.txt"), "utf-8"));
		
		bw.write(index++ + "\t" + 2 + "\t \t " + "\n");
		bw.write(index++ + "\t" + 3 + "\t \t " + "\n");
		bw.write(index++ + "\t" + 4 + "\t \t " + "\n");
		for(int hour = 0; hour <= 23; hour++){
			bw.write(index++ + "\t" + 1 + "\t" + hour + "\t" +  getTimeType(hour) + "\n");
		}
		bw.close();
	}
	
	public static String getTimeType(int hour){
		if(hour <=6 ){
			return "凌晨";
		} else if(hour <= 9){
			return "早晨";
		} else if(hour <= 12){
			return "上午";
		} else if(hour < 18){
			return "下午";
		} else 
			return "晚上";
	}
}
