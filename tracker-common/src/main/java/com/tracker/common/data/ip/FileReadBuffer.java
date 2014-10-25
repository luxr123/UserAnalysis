package com.tracker.common.data.ip;
/**
 * 文件内存类
 * 读取文件到内存中，用byte[]数组缓存，用于文件的读取
 * @author kris.chen
 *
 */
public class FileReadBuffer {
	public byte[] fileByteBuffer;
	public int readPosition;
	public int fileLength;
	
	public FileReadBuffer(int fileLength){
		this.fileLength=fileLength;
		this.fileByteBuffer=new byte[fileLength];
		readPosition=0;
	}
	//指定读指针在缓存中的位置
	public void seek(int position){
		this.readPosition=position;
	}
	//读取当前字节,读指针位置+1
	public byte readByte(){
		return this.fileByteBuffer[this.readPosition++];
	}
	//读取指定位置的字节
	public byte readByte(int position){
		return this.fileByteBuffer[position];
	}
	//从读指针的位置,读取数据到bytes字节数组中,长度为bytes字节数组长度
	public void readFully(byte[] bytes){
		int length=bytes.length;
		for(int i=0;i<length;i++){
			bytes[i]=this.fileByteBuffer[this.readPosition++];
		}
	}
	//从指定位置,读取数据到bytes字节数组中,长度为bytes字节数组长度
	public void readFully(byte[] bytes,int position){
		int length=bytes.length;
		for(int i=0;i<length;i++){
			bytes[i]=this.fileByteBuffer[position+i];
		}
	}
	//获取当前读指针位置
	public int getPosition(){
		return this.readPosition;
	}
	
	
}
