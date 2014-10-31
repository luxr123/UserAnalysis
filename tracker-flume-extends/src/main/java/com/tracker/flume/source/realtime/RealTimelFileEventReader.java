package com.tracker.flume.source.realtime;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.client.avro.ReliableEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.tracker.common.log.parser.DefaultLogParser;
import com.tracker.common.log.parser.LogParser;
import com.tracker.flume.source.realtime.FileMonitorRunnable.DirectoryTailEvent;
import com.tracker.flume.source.realtime.FileMonitorRunnable.FileEventType;

/**
 * 
 * 文件名：RealTimelFileEventReader
 * 创建人：jason.hua
 * 创建日期：2014-10-27 下午3:11:53
 * 功能描述：监控指定目录下的文件，并实时读取
 * 包括3个部分：
 * 1. 目录监听器，用于监听目录下文件的created、modify、deleted
 * 2. metaFile管理
 * 3. 日志文件实时读取
 * 监听到created的事件放入eventQueue中，待读取文件信息放入fileQueue中
 *
 */
public class RealTimelFileEventReader implements ReliableEventReader{
	private static final Logger logger = LoggerFactory
		      .getLogger(RealTimelFileEventReader.class);
	
	public static final String metaFileName = ".flume-trace.meta";
	public static final int UPDATE_META_FILE_FREQ = 2; // second
	public static final int UPDATE_META_FILE_DELAY = 0;//second
	public static final String COMPLETED_SUFFIX_VALUE = ".completed";
	public static final String DEFAULT_LOG_PARSER_CLASS = DefaultLogParser.class.getName();
	
	private String filePrefix;
	private String completedSuffix;
	
//	private ReadInfo lastReadInfo = null;
	//用于存储目标目录中创建的文件信息
	private BlockingQueue<DirectoryTailEvent> eventQueue = new LinkedBlockingQueue<DirectoryTailEvent>(100);
	//用于存储待传输的日志文件
	private BlockingQueue<ReadInfo> fileQueue = new LinkedBlockingQueue<ReadInfo>(100);
	private MetaFileManager metaFileManager = null;
	private DataFileReaderManager dataFileManager = null;
	private ExecutorService executorService;
	private ScheduledExecutorService readInfoUpdateExec;

	/**
	 * 
	 * 构造方法的描述：Create a RealTimeFileEventReader to watch the given directory
	 * 创建人：kris.chen
	 * 创建日期：2014-10-27 下午3:13:25
	 * @param trackDirPath
	 * @param filePrefix
	 * @param completedSuffix
	 * @param initialDelay
	 * @param period
	 * @param logParserClass
	 * @param header
	 */
	private RealTimelFileEventReader(String trackDirPath,
			String filePrefix,
			String completedSuffix,
			int initialDelay,
			int period,
			String logParserClass,
			String header){
		//check null 
		Preconditions.checkNotNull(trackDirPath);
		Preconditions.checkNotNull(filePrefix);
		Preconditions.checkNotNull(completedSuffix);
		Preconditions.checkNotNull(logParserClass);
		
	    this.filePrefix = filePrefix;
	    this.completedSuffix = completedSuffix;
		
	    // Verify directory exists and is readable/writable
		File trackerDirectory = new File(trackDirPath);
	    Preconditions.checkState(trackerDirectory.exists(),
	        "Directory does not exist: " + trackerDirectory.getAbsolutePath());
	    Preconditions.checkState(trackerDirectory.isDirectory(),
	        "Path is not a directory: " + trackerDirectory.getAbsolutePath());
	    
	    //Do a canary test to make sure we have access to trackDirectory
	    try{
	    	  File f1 = File.createTempFile("flume", "test", trackerDirectory);
	          Files.write("testing flume file permissions\n", f1, Charsets.UTF_8);
	          Files.readLines(f1, Charsets.UTF_8);
	          if (!f1.delete()) {
	            throw new FlumeException("Unable to delete canary file " + f1);
	          }
	    } catch (IOException e){
	    	throw new FlumeException("Unable to read and modify files" +
	    	          " in the spooling directory: " + trackerDirectory, e);
	    }
	    
	    //  monitor directory
	    executorService = Executors.newSingleThreadExecutor();
		executorService.submit(new FileMonitorRunnable(eventQueue, trackDirPath, filePrefix, completedSuffix));
		
		//扫描源数据信息，获取上次读取日志文件数据
		metaFileManager = new MetaFileManager(trackDirPath, metaFileName);
		
		//init file reader
		LogParser logParser;
		try {
			logParser = (LogParser) Class.forName(logParserClass).newInstance();
		} catch (InstantiationException e1) {
			 throw new FlumeException("Unable reflect LogParser: " + logParserClass, e1);
		} catch (IllegalAccessException e1) {
			 throw new FlumeException("Unable reflect LogParser: " + logParserClass, e1);
		} catch (ClassNotFoundException e1) {
			 throw new FlumeException("Unable reflect LogParser: " + logParserClass, e1);
		}
		
		dataFileManager = new DataFileReaderManager(logParser, header);
		
		//扫描指定日志目录
		scanDirectory(trackerDirectory);
		
		//定时更新meta file，默认为2秒更新一次
		readInfoUpdateExec = Executors.newSingleThreadScheduledExecutor();
		ScheduledFuture future = readInfoUpdateExec.scheduleAtFixedRate(new Runnable(){   
		     public void run() {   
		    	 metaFileManager.updateReadInfo(dataFileManager.getReadInfo());
		     }
			}, initialDelay, period, TimeUnit.SECONDS);   
//		try {
//			future.get();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		} catch (ExecutionException e) {
//			e.printStackTrace();
//		}
	}

	 /**
	  * 
	  * 函数名：scanDirectory
	  * 创建人：kris.chen
	  * 创建日期：2014-10-27 下午3:15:03
	  * 功能描述：扫描指定目录及metaFile，获取未完成传输的日志文件信息
	  * 文件信息按创建时间排序
	  * @param trackerDirectory
	  */
	private void scanDirectory(File trackerDirectory){
		//扫描指定目录，获取待传输日志文件
		File[] files = trackerDirectory.listFiles(new FileFilter(){
			@Override
			public boolean accept(File file) {
				String fileName = file.getName();
				if(file.isDirectory() || !fileName.startsWith(filePrefix) || fileName.endsWith(completedSuffix))
					return false;
				return true;
			}
		});
		
		if(files.length == 0)
			return;
		
		//如果meta中记录了上次读取信息，则从扫描到的File数组中去除掉上次读取的文件，此文件先进行读取
		if(metaFileManager.getLastReadInfo() != null){
			int index = -1;
			String readFileName = metaFileManager.getLastReadInfo().getFileName();
			for(int i = 0; i < files.length; i++){
				if(files[i].getName().equals(readFileName)){
					index = i;
					break;
				}
			}
			if(index < 0){
				metaFileManager.clearLastReadInfo();
			} else {
				dataFileManager.resetFile(metaFileManager.getLastReadInfo());
				files = (File[]) ArrayUtils.remove(files, index);
			}
		}
		
		//待读取的日志文件根据修改时间由小到大排序，确保最早创建的文件先传输。
		List<ReadInfo> fileInfoList = new ArrayList<ReadInfo>();
		for(int i = 0; i < files.length; i++){
			fileInfoList.add(new ReadInfo(files[i]));
		}
		Collections.sort(fileInfoList, new Comparator<ReadInfo>() {
			@Override
			public int compare(ReadInfo o1, ReadInfo o2) {
				if(o1.lastModified > o2.lastModified)
					return 1;
				else if(o1.lastModified == o2.lastModified)
					return 0;
				else 
					return -1;
			}
		});
		
		//待读取日志文件信息放入待读取队列中
		for(int i = 0; i < fileInfoList.size(); i++){
			try {
				fileQueue.put(fileInfoList.get(i));
			} catch (InterruptedException e) {
				logger.error("error : put FileInfo to fileQueue", e);
			}
		}
	}
	
	/**
	 * 从流中获取下一行数据。
	 * 如果返回null,且文件监听器监听到有新日志文件（匹配日志文件命名规范），而默认此文件已经被读取完了。 
	 */
	
	@Override
	public Event readEvent() throws IOException {
		List<Event> events = readEvents(1);
		if(!events.isEmpty()){
			return events.get(0);
		} else {
			return null;
		}
	}

	/**
	 * 从流中获取接下来的{@code numEvents}行数据
	 * 如果返回数小于numEvents行，且文件监听器监听到有新日志文件（匹配日志文件命名规范），而默认此文件已经被读取完了。 
	 */
	@Override
	public List<Event> readEvents(int numEvents) throws IOException {
		if(!dataFileManager.isValid()){
			getNextFile();
		}
		List<Event> events = dataFileManager.readEvents(numEvents);
		if(events.isEmpty() && (fileQueue.size() > 0 || eventQueue.size() > 0)){
			getNextFile();
			events = dataFileManager.readEvents(numEvents);
		}
		
		return events;
	}

	/**
	 * Clean up any state associated with this reader.
	 */
	@Override
	public void close() throws IOException {
		if(dataFileManager != null){
			dataFileManager.close();
		}
		if(metaFileManager != null){
			metaFileManager.close();
		}
		
		if(metaFileManager != null && dataFileManager != null){
			dataFileManager.mark();
			metaFileManager.updateReadInfo(dataFileManager.getReadInfo());
		}
		
		if(readInfoUpdateExec != null)
			readInfoUpdateExec.shutdown();
		
		if(executorService != null)
			executorService.shutdown();
	}

	/**
	 * Indicate to the implementation that the previously-returned events have
	 * bean successfully processed and committed.
	 */
	@Override
	public void commit() throws IOException {
		if(dataFileManager != null)
			dataFileManager.mark();
	}

	/**
	 * 设置下次需要读取的日志文件
	 */
	private void getNextFile(){
		//对于当前读取的日志文件进行重命名，标志读取完毕印记
		retireCurrentFile();
		
		//把eventQueue中数据转入待读取队列fileQueue中
		DirectoryTailEvent event = null;
		while((event = eventQueue.poll()) != null){
			if(event.getType() == FileEventType.FILE_CREATED){
				File file = new File(event.getEvent().getFile().getName().getPath());
				try {
					fileQueue.put(new ReadInfo(file));
				} catch (InterruptedException e) {
					logger.error("fileQueue put error", e);
				}
			}
		}
		//从fileQueue中获取接下来要读取的日志文件
		ReadInfo readInfo = fileQueue.poll();
		dataFileManager.resetFile(readInfo);
	}

	/**
	 * 
	 * 函数名：retireCurrentFile
	 * 创建人：kris.chen
	 * 创建日期：2014-10-27 下午3:16:12
	 * 功能描述：对已经传输完毕的日志进行重命名，以标示它已经传输完毕，日志以.completed结尾。
	 */
	private void retireCurrentFile(){
		if(dataFileManager == null || !dataFileManager.isValid()){
			return;
		}
		
		dataFileManager.close();
		
		ReadInfo readInfo = dataFileManager.getReadInfo();
		File fileToRename = new File(readInfo.getFilePath());
		File dest = new File(readInfo.getFilePath() + completedSuffix);
		logger.info("Preparing to move file {} to {}", fileToRename, dest);
		
		if(!fileToRename.exists()){
			logger.info("file {} is not exist", fileToRename);
			return;
		}
		
		//检查目标名字的文件是否存在
		if(dest.exists()){
			 String message = "File name has been re-used with different" +
			          " files. File for: " + dest;
			 logger.error(message);
		} else {
			boolean renamed = fileToRename.renameTo(dest);
			if (renamed) {
				logger.info("Successfully rolled file {} to {}", fileToRename, dest);
			} else {
	          /* If we are here then the file cannot be renamed for a reason other
	           * than that the destination file exists (actually, that remains
	           * possible w/ small probability due to TOC-TOU conditions).*/
	          String message = "Unable to move " + fileToRename + " to " + dest +
	              ". This will likely cause duplicate events. Please verify that " +
	              "flume has sufficient permissions to perform these operations.";
				 logger.error(message);
	        }
		}
	}
	
	/**
	 * 
	 * 文件名：ReadInfo
	 * 创建人：kris.chen
	 * 创建日期：2014-10-27 下午3:16:57
	 * 功能描述：读取entity object
	 *
	 */
	public static class ReadInfo {
		private String filePath;
		private String fileName;
		private long readFileLength = 0;
		private long readTime;
		private long lastModified;
		
		public ReadInfo(File file){
			this.filePath = file.getPath();
			this.fileName = file.getName();
			this.lastModified = file.lastModified();
		}
		
		public ReadInfo(long readFileLength, long readTime, String fileName, String filePath){
			this.readFileLength = readFileLength;
			this.readTime = readTime;
			this.fileName = fileName;
			this.filePath = filePath;
		}
		
		public static ReadInfo getReadInfo(String line){
			if(line == null)
				return null;
			
			String[] msgs = line.trim().split(",");
			if(msgs != null && msgs.length == 4
					&& NumberUtils.isNumber(msgs[0])
					&& NumberUtils.isNumber(msgs[1])){
				return new ReadInfo(Long.parseLong(msgs[0]), Long.parseLong(msgs[1]), msgs[2], msgs[3]);
			} 
			return null;
		}
		
		public String getMetaLine(){
			StringBuffer sb = new StringBuffer();
			sb.append(readFileLength).append(",");
			sb.append(readTime).append(",");
			sb.append(fileName).append(",");
			sb.append(filePath);
			return sb.toString();
		}
		
		public ReadInfo clone(){
			return new ReadInfo(readFileLength, readTime, fileName, filePath);
		}

		public String getFileName() {
			return fileName;
		}

		public long getReadFileLength() {
			return readFileLength;
		}

		public long getReadTime() {
			return readTime;
		}

		public String getFilePath() {
			return filePath;
		}

		public void setReadFileLength(long readFileLength) {
			this.readFileLength = readFileLength;
		}

		public void resetReadTime(){
			this.readTime = System.currentTimeMillis();
		}
		
		public String toString(){
			return readFileLength + "," + readTime + "," + fileName + "," + filePath + "," + lastModified;
		}
	}

	 /**
	  * 
	  * 文件名：Builder
	  * 创建人：kris.chen
	  * 创建日期：2014-10-27 下午3:18:00
	  * 功能描述：Special builder class for RealFileReader
	  *
	  */
	public static class Builder {
		private String trackDirPath;
		private String filePrefix;
		private String completedSuffix;
		private int initialDelay;
		private int period; 
		private String logParserClass;
		private String header;
		private String mapping;
		
	    public Builder trackDirPath(String trackDirPath) {
	        this.trackDirPath = trackDirPath;
	        return this;
	    }
		
	    public Builder completedSuffix(String completedSuffix) {
	        this.completedSuffix = completedSuffix;
	        return this;
	      }
	    
	    public Builder filePrefix(String filePrefix){
	    	this.filePrefix = filePrefix;
	    	return this;
	    }
	    
	    public Builder metaUpdateInitialDelay(int initialDelay){
	    	this.initialDelay = initialDelay;
	    	return this;
	    }
		
	    public Builder metaUpdatePeriod(int period){
	    	this.period = period;
	    	return this;
	    }
	    
	    public Builder logParserClass(String logParserClass){
	    	this.logParserClass = logParserClass;
	    	return this;
	    }
	    
	    public Builder header(String header){
	    	this.header = header;
	    	return this;
	    }
	    
//	    public Builder mapping(String mapping){
//	    	this.mapping = mapping;
//	    	return this;
//	    }
	    
		public RealTimelFileEventReader build() throws IOException {
			return new RealTimelFileEventReader(trackDirPath, filePrefix, completedSuffix, initialDelay, period, logParserClass, header);
		}
	}
}
