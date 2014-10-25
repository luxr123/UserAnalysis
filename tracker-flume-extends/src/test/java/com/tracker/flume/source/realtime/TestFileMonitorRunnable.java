package com.tracker.flume.source.realtime;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.tracker.flume.source.realtime.FileMonitorRunnable;
import com.tracker.flume.source.realtime.FileMonitorRunnable.DirectoryTailEvent;

public class TestFileMonitorRunnable {
	public static void main(String[] args) throws InterruptedException{
		String trackDirPath = System.getProperty("user.dir");
		 BlockingQueue<DirectoryTailEvent> eventQueue = new LinkedBlockingQueue<DirectoryTailEvent>(100);
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		// directory monitor
		executorService.submit(new FileMonitorRunnable(eventQueue, trackDirPath, "access", ".completed"));

		while(true){
			DirectoryTailEvent event = eventQueue.take();
			if(event == null){
				Thread.sleep(1000);
				continue;
			}else {
				System.out.println(event.getType() + " => " + event.getEvent().getFile().getName());
			}
		}
	}
}
