package com.tracker.flume.source.realtime;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;

import com.tracker.common.log.parser.ApacheLogParser;
import com.tracker.flume.source.realtime.RealTimelFileEventReader.ReadInfo;

public class TestDataFileReaderManager {
	public static void main(String[] args) throws IOException, InterruptedException {
		String trackDirPath = System.getProperty("user.dir");
		DataFileReaderManager manager = new DataFileReaderManager(new ApacheLogParser(), "value");
		ReadInfo currentReadInfo = new ReadInfo(0, System.currentTimeMillis(), "mock.log", trackDirPath + "/mock.log");
		System.out.println(manager.isValid());
		manager.resetFile(currentReadInfo);
		System.out.println(manager.isValid());
		
		while(true){
			System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
			List<Event> events = manager.readEvents(4);
			for(Event et :events){
				System.out.println(new String(et.getBody()));
			}
			System.out.println("==============================");
			Event event = manager.readEvent();
			if(event != null){
				System.out.println(new String(event.getBody()));
			} else {
				manager.close();
				break;
			}
		}
	}
}
