package com.tracker.db.dao.siteSearch;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Tuple;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.siteSearch.entity.SearchTopResTimeResult;
import com.tracker.db.dao.siteSearch.entity.SearchTopResTimeResult.ResponseTimeRecord;
import com.tracker.db.dao.siteSearch.entity.SearchTopResult;
import com.tracker.db.dao.siteSearch.entity.SearchValueParam;
import com.tracker.db.redis.JedisUtil;
import com.tracker.db.util.RowUtil;

/**
 * top统计
 * @author jason
 *
 */
public class SearchRTTopRedisDaoImpl implements SearchRTTopDao{
	public static final String SIGN_SE_TOP_RT = "se-top-rt"; //最慢响应时间
	public static final String SIGN_SE_TOP_IP = "se-top-ip"; //最多搜索次数IP
	public static final String SIGN_SE_TOP_SEARCH_VALUE = "se-top-searchValue"; //基于搜索值
	private JedisUtil jedisCache = null;

	public SearchRTTopRedisDaoImpl(JedisUtil jedisCache) {
		this.jedisCache = jedisCache;
	}
	
	@Override
	public void updateMaxRTRecord(String date, String webId,
			Integer seId, Integer searchType, ResponseTimeRecord record) {
		String key = generateKey(date, webId, "", SIGN_SE_TOP_RT, seId, searchType);
		jedisCache.SORTSET.zadd(key, record.getResponseTime(), record.toString());
	}

	@Override
	public SearchTopResTimeResult getMaxRTRecord(String date, String webId,
			Integer seId, Integer searchType, int startIndex, int offset) {
		SearchTopResTimeResult result = new SearchTopResTimeResult();
		String key = generateKey(date, webId, "", SIGN_SE_TOP_RT, seId, searchType);
		Set<String> retStr = jedisCache.SORTSET.zrevrange(key, startIndex, startIndex + offset);
		if(retStr != null){
			Iterator<String> ite = retStr.iterator();
			while(ite.hasNext()){
				result.addRecord(JsonUtil.toObject(ite.next(), ResponseTimeRecord.class));
			}
		}
		result.setTotalCount(jedisCache.SORTSET.zlength(key));
		return result;
	}
	
	@Override
	public void updateMostSearchForIp(String date, String webId,
			Integer seId, Integer searchType, String ip, long searchCount) {
		String key = generateKey(date, webId, "", SIGN_SE_TOP_IP, seId, searchType);
		jedisCache.SORTSET.zincrby(key, searchCount, ip);
	}

	@Override
	public SearchTopResult getMostSearchForIp(String date, String webId,
			Integer seId, Integer searchType, int startIndex, int offset) {
		SearchTopResult result = new SearchTopResult();
		String key = generateKey(date, webId, "", SIGN_SE_TOP_IP, seId, searchType);
		Set<Tuple> retStr = jedisCache.SORTSET.zrevrangeWithScores(key, startIndex, startIndex + offset);
		if(retStr != null){
			Iterator<Tuple> ite = retStr.iterator();
			while(ite.hasNext()){
				Tuple tuple = ite.next();
				result.addEntry(tuple.getElement(), (long)tuple.getScore());
			}
		}
		result.setTotalCount(jedisCache.SORTSET.zlength(key));
		return result;
	}

	@Override
	public void updateMostForSearchValue(String date, String webId,
			Integer seId, Integer searchType,List<SearchValueParam> params) {
		for(SearchValueParam param: params){
			String key = generateKeyForSearchValue(date, webId, SIGN_SE_TOP_SEARCH_VALUE, seId, param.getSearchConType(), searchType);
			jedisCache.SORTSET.zincrby(key, param.getSearchCount(), param.getSearchValue());
		}
	}

	@Override
	public SearchTopResult getMostForSearchValue(String date, String webId,
			Integer seId, Integer searchType, Integer searchConType,
			int startIndex, int offset) {
		SearchTopResult result = new SearchTopResult();
		String key = generateKeyForSearchValue(date, webId, SIGN_SE_TOP_SEARCH_VALUE, seId, searchConType, searchType);
		Set<Tuple> retStr = jedisCache.SORTSET.zrevrangeWithScores(key, startIndex, startIndex + offset);
		if(retStr != null){
			Iterator<Tuple> ite = retStr.iterator();
			while(ite.hasNext()){
				Tuple tuple = ite.next();
				result.addEntry(tuple.getElement(), (long)tuple.getScore());
			}
		}
		result.setTotalCount(jedisCache.SORTSET.zlength(key));
		return result;
	}

	private String generateKey(String date, String webId,  String kpi, String sign, Integer seId, Integer searchType){
		StringBuffer sb = new StringBuffer();
		sb.append(date).append(RowUtil.ROW_SPLIT);
		sb.append(webId).append(RowUtil.ROW_SPLIT);
		sb.append(sign).append(RowUtil.ROW_SPLIT);
		sb.append(kpi).append(RowUtil.ROW_SPLIT);
		sb.append(seId).append(RowUtil.ROW_SPLIT);
		sb.append(searchType == null? "": searchType.toString());
		return sb.toString();
	}
	
	private String generateKeyForSearchValue(String date, String webId, String sign, Integer seId, Integer searchConType, Integer searchType){
		StringBuffer sb = new StringBuffer();
		sb.append(date).append(RowUtil.ROW_SPLIT);
		sb.append(webId).append(RowUtil.ROW_SPLIT);
		sb.append(sign).append(RowUtil.ROW_SPLIT);
		sb.append(seId).append(RowUtil.ROW_SPLIT); 
		sb.append(searchConType).append(RowUtil.ROW_SPLIT);
		sb.append(searchType == null? "": searchType);
		return sb.toString();
	}
	
}
