package com.tracker.data.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.common.utils.ResourceLoader;
import com.tracker.data.Servers;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.data.model.DateData;
import com.tracker.db.dao.data.model.Geography;
import com.tracker.db.dao.data.model.Page;
import com.tracker.db.dao.data.model.RefSearchEngine;
import com.tracker.db.dao.data.model.SiteSearchCondition;
import com.tracker.db.dao.data.model.SiteSearchDefaultValue;
import com.tracker.db.dao.data.model.SiteSearchEngine;
import com.tracker.db.dao.data.model.SiteSearchPage;
import com.tracker.db.dao.data.model.SiteSearchPageShowType;
import com.tracker.db.dao.data.model.SiteSearchType;
import com.tracker.db.dao.data.model.UserTypeData;
import com.tracker.db.dao.data.model.VisitTypeData;
import com.tracker.db.dao.data.model.WebSite;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.db.simplehbase.request.PutRequest;
import com.tracker.db.util.Util;

public class BaseDataInit{
	private static Logger logger = LoggerFactory.getLogger(BaseDataInit.class);
	
	public void initHBaseTable(String tableName) throws IOException {
		HBaseAdmin hbaseAdmin = HbaseUtils.getHBaseAdmin(Servers.ZOOKEEPER);
		// delete and init d_referrer
		if (hbaseAdmin.tableExists(tableName)) {
			hbaseAdmin.disableTable(tableName);
			hbaseAdmin.deleteTable(tableName);
		}
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);// 建表
		tableDescriptor.addFamily(new HColumnDescriptor("data"));// 创建列族
		hbaseAdmin.createTable(tableDescriptor);
		hbaseAdmin.close();
	}
	
	/**
	 * d_date
	 * @param book
	 * @param sheetName
	 */
	public void loadDate(Workbook book, String sheetName){
		HBaseDao hbaseDao = new HBaseDao(Servers.hbaseConnection, DateData.class);
		List<PutRequest<DateData>> putRequestList = new ArrayList<PutRequest<DateData>>();
		
		Sheet sheet = book.getSheet(sheetName);
		Iterator ite = sheet.iterator();
		HSSFRow headerRow = (HSSFRow)ite.next();
		while(ite.hasNext()){
			HSSFRow row = (HSSFRow)ite.next();
			int id = (int)row.getCell(0).getNumericCellValue();
			if(id == 0){
				logger.error(sheetName + " => " + id);
			}
			String date = String.valueOf((int)row.getCell(1).getNumericCellValue());
			String calendar = String.valueOf((int)row.getCell(2).getNumericCellValue());
			int year = (int)row.getCell(3).getNumericCellValue();
			int month = (int)row.getCell(4).getNumericCellValue();
			int dayOfMonth = (int)row.getCell(5).getNumericCellValue();
			int quaterOfYear = (int)row.getCell(6).getNumericCellValue();
			int dayOfWeek = (int)row.getCell(7).getNumericCellValue();
			String weekOfYear = String.valueOf((int)row.getCell(8).getNumericCellValue());
			int isWeekEnd = (int)row.getCell(9).getNumericCellValue();
			
			DateData dateData = new DateData();
			dateData.setDate(date);
			dateData.setCalendar(calendar);
			dateData.setYear(year);
			dateData.setMonth(month);
			dateData.setDayOfMonth(dayOfMonth);
			dateData.setQuarterOfYear(quaterOfYear);
			dateData.setDayOfWeek(dayOfWeek);
			dateData.setWeekOfYear(weekOfYear);
			if(isWeekEnd > 0){
				dateData.setIsWeekEnd(true);
			} else {
				dateData.setIsWeekEnd(false);
			}
			putRequestList.add(new PutRequest<DateData>(DateData.generateRowKey(year, id), dateData));
		}
		hbaseDao.putObjectList(putRequestList);
	}
	
	/**
	 * d_geography
	 * @param book
	 * @param sheetName
	 */
	public void loadGeography(Workbook book, String sheetName){
		HBaseDao hbaseDao = new HBaseDao(Servers.hbaseConnection, Geography.class);
		List<PutRequest<Geography>> putRequestList = new ArrayList<PutRequest<Geography>>();
		
		Sheet sheet = book.getSheet(sheetName);
		Iterator ite = sheet.iterator();
		HSSFRow headerRow = (HSSFRow)ite.next();
		while(ite.hasNext()){
			HSSFRow row = (HSSFRow)ite.next();
			int id = (int)row.getCell(0).getNumericCellValue();
			if(id == 0){
				logger.error(sheetName + " => " + id);
			}
			
			Geography geo = new Geography();
			if(row.getCell(1) != null){
				int countryId = (int)row.getCell(1).getNumericCellValue();
				geo.setCountryId(countryId > 0 ? countryId: null);
			}
			if(row.getCell(2) != null){
				String country = row.getCell(2).getStringCellValue();
				geo.setCountry(country.length() > 0 ? country : null);
			}
			if(row.getCell(3) != null){
				int provinceId = (int)row.getCell(3).getNumericCellValue();
				geo.setProvinceId(provinceId > 0 ? provinceId : null);
			}
			if(row.getCell(4) != null){
				String province = row.getCell(4).getStringCellValue();
				geo.setProvince(province.length() > 0 ? province : null);
			}
			if(row.getCell(5) != null){
				String city = row.getCell(5).getStringCellValue();
				geo.setCity(city.length() > 0 ? city : null);
			}
			
			int level = (int)row.getCell(6).getNumericCellValue();
			String  remark = row.getCell(7).getStringCellValue();
			geo.setLevel(level > 0 ? level : null);
			geo.setRemark(remark.length() > 0 ? remark : null);
			putRequestList.add(new PutRequest<Geography>(Geography.generateRow(level, id), geo));
		}
		hbaseDao.putObjectList(putRequestList);
	}
	
	/**
	 * d_page
	 * @param book
	 * @param sheetName
	 */
	public void loadPage(Workbook book, String sheetName){
		HBaseDao hbaseDao = new HBaseDao(Servers.hbaseConnection, Page.class);
		List<PutRequest<Page>> putRequestList = new ArrayList<PutRequest<Page>>();
		
		Sheet sheet = book.getSheet(sheetName);
		Iterator ite = sheet.iterator();
		HSSFRow headerRow = (HSSFRow)ite.next();
		while(ite.hasNext()){
			HSSFRow row = (HSSFRow)ite.next();
			if(row.getCell(0) == null)
				continue;
			int webId = (int)row.getCell(0).getNumericCellValue();
			String pageSign = row.getCell(1).getStringCellValue();
			String pageTitle = row.getCell(2).getStringCellValue();
			String pageDesc = row.getCell(3).getStringCellValue();
			
			Util.checkZeroValue(webId);
			Util.checkNull(pageSign);
			Util.checkNull(pageTitle);
			Util.checkNull(pageDesc);

			Page page = new Page();
			page.setPageDesc(pageDesc);
			page.setPageTitle(pageTitle);
			String rowKey = Page.generateRowKey(webId, pageSign);
			putRequestList.add(new PutRequest<Page>(rowKey, page));
		}
		hbaseDao.putObjectList(putRequestList);
	}
	
	/**
	 * d_website
	 * @param book
	 * @param sheetName
	 */
	public void loadWebSite(Workbook book, String sheetName){
		HBaseDao hbaseDao = new HBaseDao(Servers.hbaseConnection, WebSite.class);
		List<PutRequest<WebSite>> putRequestList = new ArrayList<PutRequest<WebSite>>();
		
		Sheet sheet = book.getSheet(sheetName);
		Iterator ite = sheet.iterator();
	 
		HSSFRow headerRow = (HSSFRow)ite.next();
		while(ite.hasNext()){
			HSSFRow row = (HSSFRow)ite.next();
			int webId = (int)row.getCell(0).getNumericCellValue();
			String domain = row.getCell(1).getStringCellValue();
			String desc = row.getCell(2).getStringCellValue();
			String urlPrefix = row.getCell(3).getStringCellValue();
			
			Util.checkZeroValue(webId);
			Util.checkNull(domain);
			Util.checkNull(desc);
			Util.checkNull(urlPrefix);
			
			WebSite website = new WebSite();
			website.setId(webId);
			website.setDomain(domain);
			website.setDesc(desc);
			website.setUrlPrefix(urlPrefix);
			putRequestList.add(new PutRequest<WebSite>(WebSite.generateRow(webId), website));
		}
		hbaseDao.putObjectList(putRequestList);
	}
	
	/**
	 * loadSearchPageShowType
	 * @param book
	 * @param sheetName
	 */
	public void loadSearchPageShowType(Workbook book, String sheetName){
		HBaseDao hbaseDao = new HBaseDao(Servers.hbaseConnection, SiteSearchPageShowType.class);
		hbaseDao.deleteObjectByRowPrefix(SiteSearchPageShowType.generateAllRowPrefix());
		
		List<PutRequest<SiteSearchPageShowType>> putRequestList = new ArrayList<PutRequest<SiteSearchPageShowType>>();
		
		Sheet sheet = book.getSheet(sheetName);
		Iterator ite = sheet.iterator();
	 
		HSSFRow headerRow = (HSSFRow)ite.next();
		while(ite.hasNext()){
			HSSFRow row = (HSSFRow)ite.next();
			if(row.getCell(0) == null)
				continue;
			
			int webId = (int)row.getCell(0).getNumericCellValue();
			int seId = (int)row.getCell(1).getNumericCellValue();
			Integer searchType = null;
			if(row.getCell(2) != null)
			  searchType = (int)row.getCell(2).getNumericCellValue();
			String searchPage = row.getCell(3).getStringCellValue();
			int showType = (int)row.getCell(4).getNumericCellValue();
			String name = row.getCell(5).getStringCellValue();
			
			Util.checkZeroValue(webId);
			Util.checkZeroValue(seId);
			Util.checkZeroValue(showType);
			Util.checkNull(name);
			Util.checkNull(searchPage);
			
			SiteSearchPageShowType obj = new SiteSearchPageShowType();
			obj.setName(name);
			putRequestList.add(new PutRequest<SiteSearchPageShowType>(SiteSearchPageShowType.generateRow(webId, seId, searchType, searchPage, showType), obj));
		}
		hbaseDao.putObjectList(putRequestList);
	}
	
	/**
	 * loadSearchPage
	 * @param book
	 * @param sheetName
	 */
	public void loadSearchPage(Workbook book, String sheetName){
		HBaseDao hbaseDao = new HBaseDao(Servers.hbaseConnection, SiteSearchPage.class);
		List<PutRequest<SiteSearchPage>> putRequestList = new ArrayList<PutRequest<SiteSearchPage>>();
		
		Sheet sheet = book.getSheet(sheetName);
		Iterator ite = sheet.iterator();
	 
		HSSFRow headerRow = (HSSFRow)ite.next();
		while(ite.hasNext()){
			HSSFRow row = (HSSFRow)ite.next();
			if(row.getCell(0) == null)
				continue;
			
			int webId = (int)row.getCell(0).getNumericCellValue();
			int pageId = (int)row.getCell(1).getNumericCellValue();
			String searchPage = row.getCell(2).getStringCellValue();
			String desc = row.getCell(3).getStringCellValue();
			
			Util.checkZeroValue(webId);
			Util.checkZeroValue(pageId);
			Util.checkNull(searchPage);
			Util.checkNull(desc);

			SiteSearchPage obj = new SiteSearchPage();
			obj.setSearchPage(searchPage);
			obj.setDesc(desc);
			putRequestList.add(new PutRequest<SiteSearchPage>(SiteSearchPage.generateRow(webId, pageId), obj));
		}
		hbaseDao.putObjectList(putRequestList);
	}
	
	/**
	 * d_search_engine
	 * @param book
	 * @param sheetName
	 */
	public void loadSearchEngine(Workbook book, String sheetName){
		HBaseDao hbaseDao = new HBaseDao(Servers.hbaseConnection, SiteSearchEngine.class);
		List<PutRequest<SiteSearchEngine>> putRequestList = new ArrayList<PutRequest<SiteSearchEngine>>();
		
		Sheet sheet = book.getSheet(sheetName);
		Iterator ite = sheet.iterator();
	 
		HSSFRow headerRow = (HSSFRow)ite.next();
		while(ite.hasNext()){
			HSSFRow row = (HSSFRow)ite.next();
			
			int id = (int)row.getCell(0).getNumericCellValue();
			String name = row.getCell(1).getStringCellValue();
			String desc = row.getCell(2).getStringCellValue();
			
			Util.checkZeroValue(id);
			Util.checkNull(name);
			Util.checkNull(desc);
			
			SiteSearchEngine siteSE = new SiteSearchEngine();
			siteSE.setSeId(id);
			siteSE.setName(name);
			siteSE.setDesc(desc);
			putRequestList.add(new PutRequest<SiteSearchEngine>(SiteSearchEngine.generateRow(id), siteSE));
		}
		hbaseDao.putObjectList(putRequestList);
	}
	
	
	/**
	 * d_search_type
	 */
	public void loadSearchType(Workbook book, String sheetName){
		HBaseDao hbaseDao = new HBaseDao(Servers.hbaseConnection, SiteSearchType.class);
		List<PutRequest<SiteSearchType>> putRequestList = new ArrayList<PutRequest<SiteSearchType>>();
		
		Sheet sheet = book.getSheet(sheetName);
		Iterator ite = sheet.iterator();
	 
		HSSFRow headerRow = (HSSFRow)ite.next();
		while(ite.hasNext()){
			HSSFRow row = (HSSFRow)ite.next();
			
			int seId = (int)row.getCell(0).getNumericCellValue();
			int searchType = (int)row.getCell(1).getNumericCellValue();
			String name = row.getCell(2).getStringCellValue();
			String desc = row.getCell(3).getStringCellValue();
			
			Util.checkZeroValue(seId);
			Util.checkZeroValue(searchType);
			Util.checkNull(name);
			Util.checkNull(desc);
			
			SiteSearchType siteSE = new SiteSearchType();
			siteSE.setName(name);
			siteSE.setDesc(desc);
			putRequestList.add(new PutRequest<SiteSearchType>(SiteSearchType.generateRow(seId, searchType), siteSE));
		}
		hbaseDao.putObjectList(putRequestList);
	}
	
	/**
	 * d_search_condition
	 */
	public void loadSearchCondition(Workbook book, String sheetName){
		HBaseDao hbaseDao = new HBaseDao(Servers.hbaseConnection, SiteSearchEngine.class);
		List<PutRequest<SiteSearchCondition>> putRequestList = new ArrayList<PutRequest<SiteSearchCondition>>();
		
		Sheet sheet = book.getSheet(sheetName);
		Iterator ite = sheet.iterator();
	 
		HSSFRow headerRow = (HSSFRow)ite.next();
		while(ite.hasNext()){
			HSSFRow row = (HSSFRow)ite.next();
			
			int seId = (int)row.getCell(0).getNumericCellValue();
			
			HSSFCell cell = row.getCell(1);
			Integer searchType = null;
			if(cell != null)
				searchType = (int)row.getCell(1).getNumericCellValue();
			int conType = (int)row.getCell(2).getNumericCellValue();
			String name = row.getCell(3).getStringCellValue();
			String field = row.getCell(4).getStringCellValue();
			int isKeyword = (int)row.getCell(5).getNumericCellValue();
			int sortedNum = (int)row.getCell(6).getNumericCellValue();
			
			Util.checkZeroValue(seId);
			Util.checkZeroValue(conType);
			Util.checkNull(name);
			Util.checkNull(field);
			Util.checkZeroValue(sortedNum);
			
			SiteSearchCondition siteSE = new SiteSearchCondition();
			siteSE.setSeConType(conType);
			siteSE.setName(name);
			siteSE.setField(field);
			siteSE.setIsKeyword(isKeyword);
			siteSE.setSortedNum(sortedNum);
			putRequestList.add(new PutRequest<SiteSearchCondition>(SiteSearchCondition.generateRow(seId, searchType, conType), siteSE));
		}
		hbaseDao.putObjectList(putRequestList);
	}
	
	/**
	 * d_search_default_value
	 */
	public void loadSearchDefaultValue(Workbook book, String sheetName){
		HBaseDao hbaseDao = new HBaseDao(Servers.hbaseConnection, SiteSearchDefaultValue.class);
		List<PutRequest<SiteSearchDefaultValue>> putRequestList = new ArrayList<PutRequest<SiteSearchDefaultValue>>();
		
		Sheet sheet = book.getSheet(sheetName);
		Iterator ite = sheet.iterator();
	 
		HSSFRow headerRow = (HSSFRow)ite.next();
		while(ite.hasNext()){
			HSSFRow row = (HSSFRow)ite.next();
			
			int seId = (int)row.getCell(0).getNumericCellValue();
			HSSFCell cell = row.getCell(1);
			Integer searchType = null;
			if(cell != null)
				searchType = (int)row.getCell(1).getNumericCellValue();
			int conType = (int)row.getCell(2).getNumericCellValue();
			String name = row.getCell(3).getStringCellValue();
			String field = row.getCell(4).getStringCellValue();
			int defaultValue = (int)row.getCell(5).getNumericCellValue();
			
			Util.checkZeroValue(seId);
			Util.checkZeroValue(conType);
			Util.checkNull(name);
			Util.checkNull(defaultValue);
			Util.checkNull(field);
			
			SiteSearchDefaultValue siteSE = new SiteSearchDefaultValue();
			siteSE.setCh_name(name);
			siteSE.setField(field);
			siteSE.setDefaultValue(defaultValue+"");
			putRequestList.add(new PutRequest<SiteSearchDefaultValue>(SiteSearchDefaultValue.generateRow(seId, searchType, conType), siteSE));
		}
		hbaseDao.putObjectList(putRequestList);
	}
	
	/**
	 * d_user_type
	 */
	public void loadUserType(Workbook book, String sheetName){
		HBaseDao hbaseDao = new HBaseDao(Servers.hbaseConnection, UserTypeData.class);
		List<PutRequest<UserTypeData>> putRequestList = new ArrayList<PutRequest<UserTypeData>>();
		
		Sheet sheet = book.getSheet(sheetName);
		Iterator ite = sheet.iterator();
	 
		HSSFRow headerRow = (HSSFRow)ite.next();
		while(ite.hasNext()){
			HSSFRow row = (HSSFRow)ite.next();
			
			int webId = (int)row.getCell(0).getNumericCellValue();
			int userType = (int)row.getCell(1).getNumericCellValue();
			String desc = row.getCell(2).getStringCellValue();
			String en_name = row.getCell(3).getStringCellValue();
			int isLogin = (int)row.getCell(4).getNumericCellValue();
			
			Util.checkZeroValue(webId);
			Util.checkZeroValue(userType);
			Util.checkNull(desc);
			Util.checkNull(en_name);
			
			UserTypeData data = new UserTypeData();
			data.setUserType(userType);
			data.setDesc(desc);
			data.setIsLogin(isLogin);
			data.setEn_name(en_name);
			putRequestList.add(new PutRequest<UserTypeData>(UserTypeData.generateRow(webId, userType), data));
		}
		hbaseDao.putObjectList(putRequestList);
	}
	
	/**
	 * d_referrer
	 * @param book
	 * @param sheetName
	 */
	public void loadRefSE(Workbook book, String sheetName){
		HBaseDao hbaseDao = new HBaseDao(Servers.hbaseConnection, RefSearchEngine.class);
		List<PutRequest<RefSearchEngine>> putRequestList = new ArrayList<PutRequest<RefSearchEngine>>();
		
		Sheet sheet = book.getSheet(sheetName);
		Iterator ite = sheet.iterator();
	 
		HSSFRow headerRow = (HSSFRow)ite.next();
		while(ite.hasNext()){
			HSSFRow row = (HSSFRow)ite.next();
			
			int id = (int)row.getCell(0).getNumericCellValue();
			String domain = row.getCell(1).getStringCellValue();
			String desc = row.getCell(2).getStringCellValue();
			
			Util.checkZeroValue(id);
			Util.checkNull(domain);
			Util.checkNull(desc);
			
			
			RefSearchEngine data = new RefSearchEngine();
			data.setName(desc);
			data.setDomain(domain);
			putRequestList.add(new PutRequest<RefSearchEngine>(RefSearchEngine.generateRow(id), data));
		}
		hbaseDao.putObjectList(putRequestList);
	}
	
	/**
	 * d_visit_type
	 */
	public void loadVisitType(Workbook book, String sheetName){
		HBaseDao hbaseDao = new HBaseDao(Servers.hbaseConnection, VisitTypeData.class);
		List<PutRequest<VisitTypeData>> putRequestList = new ArrayList<PutRequest<VisitTypeData>>();
		
		Sheet sheet = book.getSheet(sheetName);
		Iterator ite = sheet.iterator();
	 
		HSSFRow headerRow = (HSSFRow)ite.next();
		while(ite.hasNext()){
			HSSFRow row = (HSSFRow)ite.next();
			
			int webId = (int)row.getCell(0).getNumericCellValue();
			String sign = row.getCell(1).getStringCellValue();
			//searchType
			HSSFCell cell = row.getCell(2);
			Integer searchType = null;
			if(cell != null){
				searchType = (int)cell.getNumericCellValue();
				if(searchType == 0)
					searchType = null;
			}
			
			int visitType = (int)row.getCell(3).getNumericCellValue();
			String desc = row.getCell(4).getStringCellValue();
			
			Util.checkZeroValue(webId);
			Util.checkZeroValue(visitType);
			Util.checkNull(desc);
			Util.checkNull(sign);
			
			
			VisitTypeData data = new VisitTypeData();
			data.setVisitType(visitType);
			data.setDesc(desc);
			putRequestList.add(new PutRequest<VisitTypeData>(VisitTypeData.generateRow(webId, sign, searchType), data));
		}
		hbaseDao.putObjectList(putRequestList);
	}
	
	public static void main(String[] args) throws Exception {
		BaseDataInit tableInit = new BaseDataInit();
		tableInit.initHBaseTable("d_dictionary");

		
		Workbook book = new HSSFWorkbook(ResourceLoader.getFileInputStream("data/数据字典.xls"));
		tableInit.loadDate(book, "d_date");
		tableInit.loadGeography(book, "d_geography1");
		tableInit.loadWebSite(book, "d_website");
		tableInit.loadRefSE(book, "d_ref_search_engine");
		tableInit.loadUserType(book, "d_user_type");
		tableInit.loadVisitType(book, "d_visit_type");
		tableInit.loadPage(book, "d_page");
		tableInit.loadSearchEngine(book, "d_search_engine");
		tableInit.loadSearchType(book, "d_search_type");
		tableInit.loadSearchCondition(book, "d_search_condition");
		tableInit.loadSearchDefaultValue(book, "d_search_default_value");
		tableInit.loadSearchPageShowType(book, "d_search_page_show_type");

		Servers.shutdown();
	}
}
