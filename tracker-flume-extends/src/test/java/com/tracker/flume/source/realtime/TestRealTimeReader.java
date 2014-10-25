package com.tracker.flume.source.realtime;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flume.Event;
import org.apache.flume.FlumeException;

public class TestRealTimeReader {

	public static void main(String[] args) throws InterruptedException, IOException {
		String trackDirPath = System.getProperty("user.dir");
		// tail file
		RealTimelFileEventReader reader;
		try {
			reader = new RealTimelFileEventReader.Builder()
					.trackDirPath(trackDirPath)
					.filePrefix("access")
					.completedSuffix(".completed")
					.build();
		} catch (IOException e) {
			throw new FlumeException(
					"Error instantiating RealTimelFileEventReader", e);
		}
		
		while (true) {
			List<Event> events = reader.readEvents(2);
			if (events.isEmpty()) {
				Thread.sleep(2000);
				continue;
			}
			for(Event et :events){
				System.out.println(new String(et.getBody()));
			}
			reader.commit();		
		}
	}
}
