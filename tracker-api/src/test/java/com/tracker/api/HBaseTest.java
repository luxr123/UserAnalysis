package com.tracker.api;
import java.util.Map;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.webstats.model.WebSitePageStats;


public class HBaseTest {
	public static void main(String[] args) {
//		HBaseDao websiteStatsDao = new HBaseDao(Servers.hbaseConnection, WebSiteStats.class);
//		websiteStatsDao.deleteObjectByRowPrefix("1-1-2014062", WebSiteStats.class);
//		WebSiteStats stats = websiteStatsDao.findObject("1-1-20140623------", WebSiteStats.class, null);
		
//		HBaseDao pageDao = new HBaseDao(Servers.hbaseConnection, Page.class);
//		QueryExtInfo query = new QueryExtInfo();
//		Page page = new Page();
//		page.setWebId(1);
//		query.setObj(page);
//		 Map<String,Page> map = pageDao.unWrapToMap(pageDao.findObjectAndKeyList(Page.class, query));
//		for(String key : map.keySet())
//			System.out.println(key + " => " + map.get(key));
//		
//		HBaseDao refDao = new HBaseDao(Servers.hbaseConnection, Geography.class);
//		 Map<String,Geography> map = refDao.unWrapToMap(refDao.findObjectAndKeyList(Geography.class, null));
//		for(String key : map.keySet())
//			System.out.println(key + " => " + map.get(key));
		
		
		HBaseDao dao = new HBaseDao(Servers.hbaseConnection, WebSitePageStats.class);
		 Map<String,WebSitePageStats> map = dao.unWrapToMap(dao.findObjectAndKeyList(WebSitePageStats.class, null));
		for(String key : map.keySet())
			System.out.println(key + " => " + JsonUtil.toJson(map.get(key)));
	}
}
