package com.tracker.flume.source.realtime;

import java.io.File;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

 /**
  * 
  * 文件名：FileMonitorRunnable
  * 创建人：kris.chen
  * 创建日期：2014-10-27 下午3:09:33
  * 功能描述：监听器会监听{@code trackDirPath}目录下，并以{@code filePrefix}开头，且忽略以{@code ignoreCompletedSuffix}结尾的文件
  *
  */
public class FileMonitorRunnable implements Runnable{
	private static final Logger logger = LoggerFactory
			.getLogger(FileMonitorRunnable.class);
	
	private BlockingQueue<DirectoryTailEvent> eventQueue;
	private String trackDirPath;
	private DefaultFileMonitor fileMonitor;
	
	public FileMonitorRunnable(BlockingQueue<DirectoryTailEvent> eventQueue,
			String trackDirPath,
			String filePrefix,
			String ignoreCompletedSuffix){
		this.eventQueue = eventQueue;
		this.trackDirPath = trackDirPath;
		fileMonitor = new DefaultFileMonitor(new CustomFileListener(trackDirPath, filePrefix, ignoreCompletedSuffix));
	}
	 
	@Override
	public void run() {
		fileMonitor.setRecursive(false);
		FileSystemManager fsManager;
		try {
			fsManager = VFS.getManager();
			FileObject fileObject = fsManager.resolveFile(trackDirPath);

			if (!fileObject.isReadable()) {
				logger.error("No have readable permission, "
						+ fileObject.getURL());
				return;
			}

			if (FileType.FOLDER != fileObject.getType()) {
				logger.error("Not a directory, " + fileObject.getURL());
				return;
			}

			fileMonitor.addFile(fileObject);
		} catch (FileSystemException e) {
			logger.error(e.getMessage(), e);
			throw new FlumeException("Unable to start FileMonitor", e);
		}

		fileMonitor.start();
	}

	/**
	 * 
	 * 文件名：CustomFileListener
	 * 创建人：kris.chen
	 * 创建日期：2014-10-27 下午2:59:59
	 * 功能描述：监听文件，并对于create、change、delete事件进行处理,目前只监听created事件
	 *
	 */
	private class CustomFileListener implements FileListener{
		public FileObject trackDirFileObj;
		private String filePrefix;
		private String ignoreCompletedSuffix;


		public CustomFileListener(String trackDirPath, String filePrefix, String ignoreCompletedSuffix){
			this.filePrefix = filePrefix;
			this.ignoreCompletedSuffix = ignoreCompletedSuffix;
			
			FileSystemManager fsManager;
			try {
				fsManager = VFS.getManager();
				trackDirFileObj = fsManager.resolveFile(trackDirPath);
			} catch (FileSystemException e) {
				logger.error("CustomFileListener => " + e.getMessage(), e);
			}
		}
		
		@Override
		public void fileCreated(FileChangeEvent event) throws Exception {
			if(isTargetFile(event.getFile())){
				DirectoryTailEvent dtEvent = new DirectoryTailEvent(FileEventType.FILE_CREATED, event);
				eventQueue.put(dtEvent);
			}
//			System.out.println(FileEventType.FILE_CREATED+ " => " + event.getFile().getName().getURI());
		}

		@Override
		public void fileDeleted(FileChangeEvent event) throws Exception {
//			System.out.println(FileEventType.FILE_CHANGED+ " => " + event.getFile().getName());
		}

		@Override
		public void fileChanged(FileChangeEvent event) throws Exception {
//			System.out.println(FileEventType.FILE_DELETED+ " => " + event.getFile().getName());
		}
		
		/**
		 * 监听到的文件必须是{@code trackDirPath}目录的子文件，
		 * 而且文件名必须以{@code filePrefix}开头，并不以{@code ignoreCompletedSuffix}结尾。
		 */
		private boolean isTargetFile(FileObject fileObj){
			try {
				//验证此文件是否是目录
				File file = new File(fileObj.getName().getPath());
				if(file.isDirectory()){
					return false;
				}
				//验证此文件是否是指定目录的子文件
				FileObject parentFileObj = fileObj.getParent();
				if(parentFileObj == null){
					logger.error("no file parent, " +  fileObj.getURL());
					return false;
				}
				String parentFilePath = parentFileObj.getName().getPath();
				if(trackDirFileObj != null && !trackDirFileObj.getName().getPath().equals(parentFilePath)){
					logger.info("create file is not sub file of target directory : " + fileObj.getURL());
					return false;
				}
				//验证文件名是否符合指定命名规范
				String fileName = fileObj.getName().getBaseName();
				if(fileName.startsWith(filePrefix) && !fileName.endsWith(ignoreCompletedSuffix))
					return true;
			} catch (FileSystemException e) {
				try {
					logger.error("no file parent, " +  fileObj.getURL());
				} catch (FileSystemException e1) {
					logger.error(e.getMessage(), e);
				}
			}
			return false;
		}
	}

	/**
	 *  three status : created, changed, deleted
	 */
	public static enum FileEventType {
		FILE_CREATED,
		FILE_CHANGED,
		FILE_DELETED
	}
	/**
	 * 
	 * 文件名：DirectoryTailEvent
	 * 创建人：kris.chen
	 * 创建日期：2014-10-27 下午3:03:43
	 * 功能描述：目录监控事件
	 *
	 */
	public static class DirectoryTailEvent{
		FileEventType type;
		FileChangeEvent event;
		
	    public DirectoryTailEvent( FileEventType type, FileChangeEvent event) {
	        this.type = type;
	        this.event = event;
	    }

		public FileEventType getType() {
			return type;
		}

		public FileChangeEvent getEvent() {
			return event;
		}
	}
}
