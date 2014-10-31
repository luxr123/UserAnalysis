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
  * 
  * 文件名：DataFileReaderManager
  * 创建人：jason.hua
  * 创建日期：2014-10-27 下午3:11:18
  * 功能描述：日志文件读取管理
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
	/**
	 * 函数名：resetFile
	 * 创建人：kris.chen
	 * 创建日期：2014-10-27 下午2:29:52
	 * 功能描述：从readInfo中重置文件读取管理对象
	 * @param readInfo
	 */
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

	/**
	 * 函数名：readEvent
	 * 创建人：kris.chen
	 * 创建日期：2014-10-27 下午2:31:31
	 * 功能描述：读取文件中的一行作为一个event
	 * @return Event
	 * @throws IOException
	 */
	public Event readEvent() throws IOException{
		String line = readLine();	//读取一行数据
		if(line == null || line.length() == 0){
			return null;
		}
		//TODO 如果解析失败，怎么处理
		try{
			LogResult result = logParser.parseLog(line);	//转换成LogResult
			if(result == null)
				return null;
			Map<String, String> headers = new HashMap<String, String>();
			headers.put(header, result.getLogTypeMappring());	//设置header
			return EventBuilder.withBody(result.getLogJson(), outputCharset, headers);	//result转成json格式合并header转换成Event
		} catch(Exception e){
			logger.error("error to parse log:" + line, e);
		}
		return null;
	}
	/**
	 * 函数名：readEvents
	 * 创建人：kris.chen
	 * 创建日期：2014-10-27 下午2:33:32
	 * 功能描述：读取numEvents个Event
	 * @param numEvents
	 * @return List<Event>
	 * @throws IOException
	 */
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
	
	/**
	 * 
	 * 函数名：readLine
	 * 创建人：kris.chen
	 * 创建日期：2014-10-27 下午2:34:56
	 * 功能描述：通过RandomAccessFile对象读取一行数据
	 * @return
	 * @throws IOException
	 */
	private String readLine() throws IOException{
		if(!isValid())
			return null;
		return randomReadFile.readLine();
	}
	
	/**
	 * 
	 * 函数名：mark
	 * 创建人：kris.chen
	 * 创建日期：2014-10-27 下午2:36:13
	 * 功能描述：Indicating that the events previously
	 * 			returned by this readEvent have been successfully committed.
	 */
	public void mark(){
		if(randomReadFile != null)
			try {
				lastTimeReadLength = randomReadFile.getFilePointer();
			} catch (IOException e) {
				logger.info("mark error", e);
			}
	}
	
	/**
	 * 
	 * 函数名：getReadInfo
	 * 创建人：kris.chen
	 * 创建日期：2014-10-27 下午2:37:17
	 * 功能描述：设置readInfo中读取文件位置
	 * @return
	 */
	public ReadInfo getReadInfo() {
		if(readInfo == null){
			return null;
		}
		readInfo.resetReadTime();
		readInfo.setReadFileLength(lastTimeReadLength);
		return readInfo;
	}
	
	/**
	 * 
	 * 函数名：isValid
	 * 创建人：kris.chen
	 * 创建日期：2014-10-27 下午2:41:26
	 * 功能描述：判断DataFileReaderManager对象是否有效
	 * @return
	 */
	public boolean isValid(){
		if(randomReadFile == null || readInfo == null){
			return false;
		}
		return true;
	}
	
	/**
	 * 
	 * 函数名：close
	 * 创建人：kris.chen
	 * 创建日期：2014-10-27 下午2:42:04
	 * 功能描述：关闭对象
	 */
	public void close(){
		try {
			if(randomReadFile != null)
				randomReadFile.close();
		} catch (IOException e) {
			logger.info("randomReadFile close error", e);
		}
	}
}
