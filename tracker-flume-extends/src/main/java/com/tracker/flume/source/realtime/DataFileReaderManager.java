package com.tracker.flume.source.realtime;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.tracker.common.log.parser.LogParser;
import com.tracker.common.log.parser.LogParser.LogResult;
import com.tracker.flume.source.realtime.RealTimelFileEventReader.ReadInfo;

/**
 * 日志文件读取管理
 * @author jason.hua
 *
 */
public class DataFileReaderManager {
	private static final Logger logger = LoggerFactory.getLogger(DataFileReaderManager.class);
	
	private Charset outputCharset = Charset.forName("UTF-8");
	
	private RandomAccessFile randomReadFile = null;   
	private ReadInfo readInfo = null;
	private long lastTimeReadLength;
	private LogParser logParser;
	private String header = "logType";
	
	public DataFileReaderManager(LogParser logParser, String header){
		this.logParser = logParser;
		this.header = header;
	}
	
 	public void resetFile(ReadInfo readInfo){
		init();
		if(readInfo == null)
			return;
		this.readInfo = readInfo.clone();
		File file = new File(readInfo.getFilePath());
		if(file.exists()){
			try {
				this.lastTimeReadLength = readInfo.getReadFileLength();
				randomReadFile = new RandomAccessFile(file,"r");
				//获得变化部分的   
				randomReadFile.seek(lastTimeReadLength);
			} catch (FileNotFoundException e) {
				logger.error("file not found：" + readInfo.getFilePath(), e);
			} catch (IOException e) {
				logger.error("error seek ：" + lastTimeReadLength, e);
			}   
		}
	}
	
	private void init(){
		this.randomReadFile = null;
		this.readInfo = null;
	}

	public Event readEvent() throws IOException{
		String line = readLine();

		if(line == null || line.length() == 0){
			return null;
		}

		//TODO 如果解析失败，怎么处理
		try{
			LogResult result = logParser.parseLog(line);
			if(result == null)
				return null;
			Map<String, String> headers = new HashMap<String, String>();
			headers.put(header, result.getLogTypeMappring());
			return EventBuilder.withBody(result.getLogJson(), outputCharset, headers);
		} catch(Exception e){
			logger.error("error to parse log:" + line, e);
		}
		return null;
	}
	
	public List<Event> readEvents(int numEvents) throws IOException{
		List<Event> events = Lists.newLinkedList();
	    for (int i = 0; i < numEvents; i++) {
	        Event event = readEvent();
	        if (event != null) {
	          events.add(event);
	        } else {
	          break;
	        }
	      }
	    return events; 
	}
	
	private String readLine() throws IOException{
		if(!isValid())
			return null;
		return randomReadFile.readLine();
	}
	
	/**
	 * Indicating that the events previously
	 * returned by this readEvent have been successfully committed.
	 */
	public void mark(){
		if(randomReadFile != null)
			try {
				lastTimeReadLength = randomReadFile.getFilePointer();
			} catch (IOException e) {
				logger.info("mark error", e);
			}
	}
	
	public ReadInfo getReadInfo() {
		if(readInfo == null){
			return null;
		}
		readInfo.resetReadTime();
		readInfo.setReadFileLength(lastTimeReadLength);
		return readInfo;
	}
	
	public boolean isValid(){
		if(randomReadFile == null || readInfo == null){
			return false;
		}
		return true;
	}
	
	public void close(){
		try {
			if(randomReadFile != null)
				randomReadFile.close();
		} catch (IOException e) {
			logger.info("randomReadFile close error", e);
		}
	}
}
