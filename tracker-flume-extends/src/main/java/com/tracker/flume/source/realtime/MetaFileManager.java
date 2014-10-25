package com.tracker.flume.source.realtime;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.flume.source.realtime.RealTimelFileEventReader.ReadInfo;

/**
 * 管理meta数据
 * 1. 使用两条记录在一定程度上来确保数据的精准性,第一行是上次readInfo数据，第二行数上上次readInfo数据
 * 2. 在更新meta数据的时候，先更新第二行数据，然后再更新第一行数据
 * 
 * @author jason.hua
 *
 */
public class MetaFileManager {
	private static final Logger logger = LoggerFactory
		      .getLogger(MetaFileManager.class);
	private final int lineLength = 100; // meta中每行数据长度
	private final int CURRENT_READ_INFO_STR_INDEX = 0;
	private final int LAST_READ_INFO_STR_INDEX = lineLength;
	private String metaFileName;
	private String trackDirPath;
	private ReadInfo lastReadInfo = null;
	private RandomAccessFile randomMetaFile = null;
	
	public MetaFileManager(String trackDirPath, String metaFileName){
		this.trackDirPath = trackDirPath;
		this.metaFileName = metaFileName;
		initMetaAccessFile();
		initReadInfo();
	}
	
	private void initMetaAccessFile(){
		File metaFile = new File(trackDirPath, metaFileName);
		try {
			if(!metaFile.exists())
				metaFile.createNewFile();
			randomMetaFile = new RandomAccessFile(metaFile,"rw");
		} catch (FileNotFoundException e) {
			throw new FlumeException("can not find metaFile", e);
		} catch (IOException e) {
			throw new FlumeException("can not create metaFile:" + metaFile.getName(), e);
		}
	}
	
	/**
	 *  从metaFile中获取用户读取日志文件信息，并对其进行验证
	 */
	private void initReadInfo(){
		ReadInfo lastAndLastReadInfo = null;
		try {
			randomMetaFile.seek(CURRENT_READ_INFO_STR_INDEX);
			String firstLine = randomMetaFile.readLine();
			randomMetaFile.seek(LAST_READ_INFO_STR_INDEX);
			String secondLine = randomMetaFile.readLine();
			
			lastReadInfo = ReadInfo.getReadInfo(firstLine);
			lastAndLastReadInfo = ReadInfo.getReadInfo(secondLine);
			
		} catch (IOException e) {
			logger.error("can not read metaFile", e);
		}
		
		//验证读取信息是否正确
		boolean isLastInfoValid = verifyReadInfo(lastReadInfo);
		boolean isLastAndLastInfoValid = verifyReadInfo(lastAndLastReadInfo);
		
		if(!isLastInfoValid && isLastAndLastInfoValid){
			lastReadInfo = lastAndLastReadInfo.clone();
		}
	}
	
	/**
	 * 验证数据是否正确，有没有缺失
	 * metaFile中行格式：readFileLength + "," + readTime + "," + filePath + "," + readFileName，
	 * 其中filePath放在行最后，所以只要验证filePath的文件是否存在，则可以知道此readInfo是否有效。
	 */
	private boolean verifyReadInfo(ReadInfo readInfo){
		if(readInfo == null)
			return false;
		
		File file = new File(readInfo.getFilePath());
		if(file.exists())
			return true;
		return false;
	}
	
	/**
	 * 更新ReadInfo数据到metaFile中
	 */
	public void updateReadInfo(ReadInfo currentReadInfo){
		if(currentReadInfo == null)
			return;
		if(lastReadInfo != null && (lastReadInfo.getReadFileLength() == currentReadInfo.getReadFileLength()))
			return;
		try {
			if(lastReadInfo != null){
				String lastReadInfoStr = String.format("%1$-" + lineLength + "s", lastReadInfo.getMetaLine() + "\n");
				randomMetaFile.seek(LAST_READ_INFO_STR_INDEX);
				randomMetaFile.write(lastReadInfoStr.getBytes());
			}
			
			if(currentReadInfo != null){
				String currentReadInfoStr = String.format("%1$-" + lineLength + "s", currentReadInfo.getMetaLine() + "\n");
				randomMetaFile.seek(CURRENT_READ_INFO_STR_INDEX);
				randomMetaFile.write(currentReadInfoStr.getBytes());
				this.lastReadInfo = currentReadInfo.clone();
			}
		} catch (IOException e) {
			logger.error("error to  write data :" + metaFileName, e);
			close();
			initMetaAccessFile();
		}
	}
	
	
	public void close(){
		if(randomMetaFile != null)
			try {
				randomMetaFile.close();
			} catch (IOException e) {
				logger.error("error to close randomMetaFile", e);
			}
	}
	
	public ReadInfo getLastReadInfo() {
		return lastReadInfo;
	}
	
	public void clearLastReadInfo(){
		lastReadInfo = null;
	}
}
