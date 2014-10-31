package com.tracker.storm.drpc.groupstream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import com.tracker.common.utils.EasyPartion;
import com.tracker.common.utils.StringUtil;
import com.tracker.storm.drpc.TransportBolt;
/**
 * 
 * 文件名：PartitionGroup
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午5:28:26
 * 功能描述：用于实时访客查询请求的转发类.复制每个请求,并追加不同的数据
 * 分区号,复制请求的数量为SearchRealTimeStatisticBolt的Execute数量且小于分区
 * 的总数.
 *
 */
public class LinePartitionGroup implements GroupStream,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7851331564252172452L;
	

	@Override
	/**
	 * 1创建一个分区列表,列表的每条记录代表一个处理bolt的execute,每个条记录会被分配分区,如果execute数
	 * 大于分区数,那么部分execute无法获得分区
	 * 2将分区列表的记录与请求拼接.那么每条记录将由一个execute去执行,记录的总数小于execute的实际数量和
	 * 分区的总数.
	 */
	public List<Object> group(TransportBolt transport, String request) {
		// TODO Auto-generated method stub
		// topsearchvalue:engine:field:startindex:endindex
		List<Object> retVal = new ArrayList<Object>();
		List<String> partList = new ArrayList<String>();
		for (int j = 0; j < EasyPartion.partitions; ) {
			for (int i = 0; i < transport.getTransportSize() && j < EasyPartion.partitions; i++) {
				partList.add(StringUtil.KEY_VALUE_SPLIT + j++);
			}
		}
		int preLength = 0;
		for (int j = 0; j < EasyPartion.partitions; ) {
			for(int i = 0; i< transport.getTransportSize() && j < EasyPartion.partitions;){
				if(j!=0 &&j % 3 == 0){
					i = i++ >=transport.getTransportSize() ? 0:i;
					preLength = 0;
				}
				String tmp = partList.get(j++);
				if(preLength != 0 && preLength != tmp.length()){
					i++;
				}
				preLength = tmp.length();
				if(retVal.size() > i){
					String tmpReq = (String)retVal.get(i);
					String passStr = tmpReq + tmp;
					retVal.set(i,passStr);
				}else{
					tmp = tmp.substring(1, tmp.length());
					String passStr = request + StringUtil.ARUGEMENT_SPLIT + tmp;
					retVal.add(passStr);
				}
			}
		}
		return retVal;
	}
}
