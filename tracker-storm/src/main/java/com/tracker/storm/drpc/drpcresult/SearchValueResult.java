package com.tracker.storm.drpc.drpcresult;
//this is no longer used just for sampling
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.tracker.common.utils.StringUtil;
/**
 * 
 * 文件名：SearchValueResult
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午5:25:57
 * 功能描述：
 *
 */
public class SearchValueResult extends DrpcResult implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2914403118979973328L;
	private List<ValueItem> m_list;
	private Integer m_total;
	
	
	public SearchValueResult(List<ValueItem> list,Integer count){
		m_list = list;
		m_total = count;
		m_responseType = 11;
	}
	public List<ValueItem> getList(){
		return m_list;
	}
	
	@Override
	public DrpcResult merge(DrpcResult part) {
		// TODO Auto-generated method stub
		SearchValueResult svr = (SearchValueResult)part;
		m_total += svr.getTotal();
		Integer size = m_list.size();
		Integer input_size = svr.getList().size();
		List<ValueItem> tmp = new ArrayList<SearchValueResult.ValueItem>();
		Long count = 0L,input_count = 0L;
		for(int i = 0,j = 0;i<size || j< input_size;){
			count = i >= size ? 0L : m_list.get(i).getCount();
			input_count = j >= input_size ? 0L:svr.getList().get(j).getCount();
			if(i > size || (j < input_size && count < input_count)){
				tmp.add(svr.getList().get(j++));
			}else if(i < size){
				tmp.add(m_list.get(i++));
			}
		}
		m_list = tmp;
		return this;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		String retVal = Integer.toString(m_total) + StringUtil.RETURN_ITEM_SPLIT;
		for(ValueItem item : m_list){
			retVal += item.toString() + StringUtil.RETURN_ITEM_SPLIT;
		}
		return retVal;
	}
	
	public Integer getTotal(){
		return m_total;
	}
	/**
	 * 
	 * 文件名：ValueItem
	 * 创建人：zhifeng.zheng
	 * 创建日期：2014年10月23日 下午4:05:25
	 * 功能描述：保存索引表返回的每行记录,包括基础表行健,时间戳
	 *
	 */
	public static class ValueItem  implements Serializable{
		protected String m_name;
		protected Long m_count;
		public ValueItem(String name,Long count){
			m_name = name;
			m_count = count;
		}
		public String getName() {
			return m_name;
		}

		public Long getCount() {
			return m_count;
		}
		
		public void setCount(Long count){
			m_count = count;
		}
		
		public String toString(){
			return m_name + StringUtil.KEY_VALUE_SPLIT + m_count;
		}
	}
	/**
	 * 
	 * 文件名：ValueItem_hext
	 * 创建人：zhifeng.zheng
	 * 创建日期：2014年10月23日 下午4:05:46
	 * 功能描述：保存索引表返回的每条记录,包括基础表行健,时间戳,访问数量
	 *
	 */
	public static class ValueItem_hext  extends ValueItem implements Serializable{
		/**
		 * 
		 */
		private Long m_timeStamp;
		private static final long serialVersionUID = -959044933736222375L;
		
		public ValueItem_hext(String name, Long timeStamp,Long count) {
			super(name, count);
			// TODO Auto-generated constructor stub
			m_timeStamp = timeStamp;
		}
		
		public long getTimestamp(){
			return m_timeStamp;
		}
		
		@Override
		public String toString(){
			return m_name + StringUtil.KEY_VALUE_SPLIT + m_timeStamp 
					+ StringUtil.ARUGEMENT_SPLIT +  m_count;
		}
	}

	@Override
	public Integer responseType() {
		// TODO Auto-generated method stub
		return 11;
	}
}
