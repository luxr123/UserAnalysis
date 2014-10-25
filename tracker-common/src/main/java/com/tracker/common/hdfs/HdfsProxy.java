package com.tracker.common.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
/**
 * 
 * 文件名：HdfsProxy
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月24日 上午11:16:43
 * 功能描述：提供从hdfs中读取文件的方法
 *
 */
public class HdfsProxy {
	public static final String DEFAULT_FSLOCATION="hdfs://10.100.2.94";
	public static String FSLOCATION = null;
	
	public static void setFSLocation(String location){
		FSLOCATION = location;
	}
	
	public byte[] readFileFromHdfs(String path){
		String hdfsLocation = null;
		byte[] buffer = null;
		if(FSLOCATION == null)
			hdfsLocation = DEFAULT_FSLOCATION;
		else
			hdfsLocation = FSLOCATION;
		FSDataInputStream fsinput = null;
		FileSystem fs = null;
		try{
			Configuration conf = new Configuration();
			conf.set(FileSystem.FS_DEFAULT_NAME_KEY, hdfsLocation);
			fs = FileSystem.get(conf);
			// parse hdfs server from zookeeper
//			Hdfs hdfs = (Hdfs) AbstractFileSystem.createFileSystem(new URI(hdfsLocation), conf);
			// read and write
//			fsinput = hdfs.open(new Path(path));
			fsinput = fs.open(new Path(path));
			if (fsinput != null) {
				buffer = new byte[fsinput.available()];
				int off = 0,len = 0;
				int length = buffer.length;
				for(;;){
					try {
						len = fsinput.read(buffer,off,length);
						length -= len;
						off += len;
						if(off <= 0 || length <= 0)
							break;
					} catch (Exception e) {
						break;
					}
				}
			}
			
		}
		catch(Exception e){
			System.out.println("error while read file " + path);
		} finally {
			IOUtils.closeStream(fsinput);
			try {
				if(fs != null)
					fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return buffer;
	}
}
