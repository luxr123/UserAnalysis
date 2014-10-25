package com.tracker.flume.source.realtime;

import com.tracker.flume.source.realtime.RealTimelFileEventReader.ReadInfo;


public class TestMetaFileManager {
	public static void main(String[] args) {
		String trackDirPath = System.getProperty("user.dir");
		MetaFileManager metaFileManager = new MetaFileManager(trackDirPath, "flume-trace.meta");
//		System.out.println(metaFileManager.getLastReadInfo());
		
		ReadInfo currentReadInfo = new ReadInfo(5, System.currentTimeMillis(), "mock.log", trackDirPath + "/mock.log");
		metaFileManager.updateReadInfo(currentReadInfo);
		System.out.println(metaFileManager.getLastReadInfo());
	}
}
