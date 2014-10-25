package com.tracker.hive.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tracker.common.constant.website.ReferrerType;
import com.tracker.common.constant.website.SysEnvType;
import com.tracker.common.utils.JsonUtil;
import com.tracker.hive.service.entity.PageEntity;
import com.tracker.hive.service.entity.SessionEntity;
import com.tracker.hive.service.entity.SysEnvEntity;

/**
 * 会话服务类
 * @author jason.hua
 * 
 */
public class SessionService {
	private List<String> sessionEntities = new ArrayList<String>();
	private SessionEntity lastSessionEntity = null;
	private PageEntity lastPageEntity = null;
	private long lastPageViewTime = 0;
	private long sessionTime = 0;
	
	private boolean running = false;
	
	
	/**
	 * 初始化
	 */
	private void init(){
		lastSessionEntity = null;
		lastPageEntity = null;
		lastPageViewTime = 0;
		sessionTime = 0;
		sessionEntities = new ArrayList<String>(); 
	}
	
	/**
	 * 添加log
	 */
	public void addLog(long ckct, String pageSign, long visitTime, int refType, String refKeyword, String refDomain, String refSubDomain, Map<SysEnvType, String> sysEnvMap) {
		//如果当前实体不处于running状态，则初始化实体
		if(!running){
			init();
			running = true;
		}
		
		// 如果来源是外部链接，或者是搜索引擎，则开始新的会话
		if (ReferrerType.OTHER_LINK.getValue() == refType || ReferrerType.SEARCH_ENGINE.getValue() == refType) {
			nextSession(ckct, pageSign, visitTime, refType, refKeyword, refDomain, refSubDomain, sysEnvMap);
			return;
		}
		// 处理直接访问log
		if (lastPageEntity == null || lastPageViewTime == 0) {
			nextSession(ckct, pageSign, visitTime, refType, refKeyword, refDomain, refSubDomain, sysEnvMap);
		} else {
			// 获取在页面上停留的时间
			long timeGap = computePageStayTime(visitTime);
			// 如果停留时间大于30分钟，则开始新的会话
			if (timeGap > 30 * 60) {
				nextSession(ckct, pageSign, visitTime, refType, refKeyword, refDomain, refSubDomain, sysEnvMap);
			} else {
				nextPage(pageSign, visitTime);
			}
		}
	}

	/**
	 * 计算页面停留时间
	 * @param visitTime
	 * @return 秒
	 */
	private long computePageStayTime(long visitTime) {
		return lastPageViewTime == 0 ? 0 : (visitTime - lastPageViewTime) / 1000;
	}

	/**
	 * 在一次会话内，页面访问
	 * @param url
	 * @param title
	 * @param visitTime
	 */
	private void nextPage(String pageSign, long visitTime) {
		lastPageEntity.setNextPageSign(pageSign); //nextPage
		lastPageEntity.setNextPageCount(1);//贡献下游浏览次数
		lastPageEntity.setStayTime(computePageStayTime(visitTime));// 上次页面总停留时长（second）
		
		sessionTime += lastPageEntity.getStayTime(); //计算会话时间
		lastSessionEntity.addPageEntity(lastPageEntity); //将Page实体添加到会话实体中
		lastSessionEntity.incrementTotalPage(); //总的访问页数加1 
		
		this.lastPageViewTime = visitTime; //记录本次页面的访问时间
		
		// 初始化本次页面
		lastPageEntity = new PageEntity();
		lastPageEntity.setPageSign(pageSign);
		lastPageEntity.setPv(1);// 浏览数
	}

	/**
	 * 上次会话结束，开始下个会话
	 */
	private void nextSession(long ckct, String pageSign, long visitTime, int refType, String refKeyword, String refDomain, String refSubDomain, Map<SysEnvType, String> sysEnvMap){
		// 为上次页面访问赋值，并add到上次session中
		if (lastSessionEntity != null && lastPageEntity != null) {
			lastPageEntity.setOutPageCount(1);// 出口页次数
			
			if (lastSessionEntity.getTotalPage() == 0){
				lastPageEntity.setJumpCount(1);// 页面级， 跳出次数
				lastSessionEntity.setJumpCount(1);//会话级， 跳出次数， jumpCount
			}
			
			lastPageEntity.setStayTime(computePageStayTime(visitTime));// 总停留时长（second）

			
			sessionTime += lastPageEntity.getStayTime(); //计算会话时间
			
			lastSessionEntity.setSessionTime(sessionTime); // sessionTime
			lastSessionEntity.setTotalPage(lastSessionEntity.getPageEntities().size()); //totalPage
			//add到list中
			lastSessionEntity.addPageEntity(lastPageEntity); // pageEntities
			//添加到会话实体集合
			sessionEntities.add(JsonUtil.toJson(lastSessionEntity));
		}

		// 初始化本次session
		lastSessionEntity = new SessionEntity();
		Integer[] ids = FieldParser.parseDate(visitTime);
		if(ids != null && ids.length == 2){
			lastSessionEntity.setServerDateId(ids[0]); //serverDateId
			lastSessionEntity.setServerTimeId(ids[1]); //serverTimeId
		}
		lastSessionEntity.setRefType(refType);
		lastSessionEntity.setDomain(refDomain);//refId
		lastSessionEntity.setRefKeyword(refKeyword);//refKeyword
		lastSessionEntity.setPageSign(pageSign);//会话入口页面pageId
		//获取在日、周、月、年的访客类型
		int[] visitorType = FieldParser.parseCookieCreateTime(ckct, visitTime);
		lastSessionEntity.setVisitorTypeOfDay(visitorType[0]);
		lastSessionEntity.setVisitorTypeOfWeek(visitorType[1]);
		lastSessionEntity.setVisitorTypeOfMonth(visitorType[2]);
		lastSessionEntity.setVisitorTypeOfYear(visitorType[3]);
		
		//会话时间初始化
		sessionTime = 0;
		
		//系统环境
		SysEnvEntity sysEnvEntity = new SysEnvEntity();
		sysEnvEntity.setResult(sysEnvMap);
		lastSessionEntity.setSysEnvEntity(sysEnvEntity); //sysEnvEntity

		// 初始化本次入口页page
		lastPageEntity = new PageEntity();
		lastPageEntity.setPageSign(pageSign);//pageId
		lastPageEntity.setPv(1);// 浏览数
		lastPageEntity.setEntryPageCount(1);// 入口页次数
		
		this.lastPageViewTime = visitTime;//记录本次页面的访问时间
	}
	

	/**
	 * 终止会话计算
	 */
	public void endHandle() {
		if (lastSessionEntity != null && lastPageEntity != null) {
			if (lastSessionEntity.getTotalPage() == 0) {
				lastPageEntity.setJumpCount(1);// 跳出次数
				lastPageEntity.setOutPageCount(1);// 出口页次数
			}
			lastSessionEntity.addPageEntity(lastPageEntity);

			lastSessionEntity.setSessionTime(sessionTime);
			lastSessionEntity.setTotalPage(lastSessionEntity.getPageEntities().size());
			
			//添加到会话实体集合
			sessionEntities.add(JsonUtil.toJson(lastSessionEntity));
		}
		running = false;
	}

	/**
	 * 函数名：getSessionEntities
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-21 下午5:31:54
	 * 功能描述：返回会话实体集合
	 * @return
	 */
	public List<String> getSessionEntities() {
		return sessionEntities;
	}
	
	@Override
	public String toString() {
		return getSessionEntities().toString();
	}
	

	public static void main(String[] args) {
		SessionService service = new SessionService();
		Map<SysEnvType, String> sysEnvMap = new HashMap<SysEnvType, String>();
		sysEnvMap.put(SysEnvType.BROWSER, "chrome");
		sysEnvMap.put(SysEnvType.OS, "win7");
		
		service.addLog(1401081791, 1+"", System.currentTimeMillis(),1, "", "", "", sysEnvMap);
		service.addLog(System.currentTimeMillis() / 1000, 2+"",System.currentTimeMillis() + 1000,2, "baidu.com", "", "", sysEnvMap);
		service.addLog(System.currentTimeMillis() / 1000, 3+"",System.currentTimeMillis() + 2000,1, "", "", "", sysEnvMap);
		service.endHandle();
		List<String> sessions = service.getSessionEntities();
		for(String session: sessions)
			System.out.println(session);
	}
}
