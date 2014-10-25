package com.tracker.flume.source.realtime;

import java.io.IOException;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;

import com.tracker.common.log.parser.ApacheLogParser;


public class TestDirectoryTailSource {
	public static void main(String[] args) throws InterruptedException, IOException {
		String trackDirPath = System.getProperty("user.dir");
		
		DirectoryTailSource source = new DirectoryTailSource();
		Context context = new Context();
		context.put(DirectoryTailSource.TRACK_DIR_PATH, trackDirPath);
		context.put(DirectoryTailSource.FILE_PREFIX, "access");
		context.put(DirectoryTailSource.LOG_PARSER_CLASS, ApacheLogParser.class.getName());
		context.put(DirectoryTailSource.BATCH_SIZE, "2");
		context.put(DirectoryTailSource.SELECTOR_HEADER, "logType");
		context.put(DirectoryTailSource.SELECTOR_MAPPING, "apacheLog");
		source.setChannelProcessor(new CustomChannelProcessor());
		source.configure(context);
		source.start();
	}
	
	static class CustomChannelProcessor extends ChannelProcessor{

		public CustomChannelProcessor() {
			super(null);
		}

		@Override
		public void processEventBatch(List<Event> events) {
			for(Event event :events){
				System.out.println(new String(event.getBody()) + " " + event.getHeaders());
			}
		}

		@Override
		public void processEvent(Event event) {
			System.out.println(new String(event.getBody()) + " " + event.getHeaders());
		}
		
	}
}
