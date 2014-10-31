package com.tracker.storm.drpc.drpcresult;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.tracker.common.utils.StringUtil;
import com.tracker.storm.drpc.drpcresult.SearchValueResult.ValueItem;
import com.tracker.storm.drpc.drpcresult.SearchValueResult.ValueItem_hext;
/**
 * 
 * 文件名：RTVisitorResult
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午5:25:12
 * 功能描述：实时访客查询的结果类.
 *
 */
public class RTVisitorResult extends DrpcResult implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -6780684071165431506L;
	List<ValueItem_hext> m_list;
	Integer m_total;
	
	public RTVisitorResult(List<ValueItem_hext> buf,Integer count){
		m_list = new ArrayList<SearchValueResult.ValueItem_hext>(buf);
		m_total = count;
	}
	
	@Override
	public DrpcResult merge(DrpcResult part) {
		// TODO Auto-generated method stub
		if(part != null){
			RTVisitorResult rtvr = (RTVisitorResult)part;
			if(m_total >= 0)
			m_total += rtvr.getTotal();
			Integer size = m_list.size();
			Integer input_size = rtvr.getBuff().size();
			List<ValueItem_hext> tmp = new ArrayList<SearchValueResult.ValueItem_hext>();
			Long timeStamp = 0L,input_timeStamp = 0L;
			for(int i = 0,j = 0;i<size || j< input_size;){
				timeStamp = i >= size ? 0L : m_list.get(i).getTimestamp();
				input_timeStamp = j >= input_size ? 0L: rtvr.getBuff().get(j).getTimestamp();
				if(i >= size || (j < input_size && timeStamp < input_timeStamp)){
					tmp.add(rtvr.getBuff().get(j++));
				}else if(i < size){
					tmp.add(m_list.get(i++));
				}else{
					j++;
				}
			}
			m_list = tmp;
		}
		return this;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		String retVal = Integer.toString(m_total) + StringUtil.RETURN_ITEM_SPLIT;
		for(ValueItem item : m_list){
			retVal += item.getName()  + StringUtil.ARUGEMENT_END + 
					item.getCount() + StringUtil.RETURN_ITEM_SPLIT;
		}
		return retVal;
	}

	@Override
	public Integer responseType() {
		// TODO Auto-generated method stub
		return 19;
	}
	
	public List<ValueItem_hext> getBuff(){
		return m_list;
	}
	
	public Integer getTotal(){
		return m_total;
	}
}
