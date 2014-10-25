package com.tracker.db.table;

import java.util.ArrayList;
import java.util.List;

public class ApachePVDesc implements BaseTableDesc{
		private String m_logFields[][] = {
			{ "infomation", "city", "curUrl", "ip", "os", "browser","title","userId","userType",
					"isCookieEnable", "language", "screen", "cookieId" ,"colorDepth","count","country","province"},
					{"extinfomation","visitTime", "referrer", "refType", "refDomain" ,"webId","visitStatus"}};
		private  enum FIELDS{
			webId,city, visitTime, curUrl, ip, os, browser, isCookieEnable, language, screen, cookieId, isNewVisitor, refType, refDomain,referrer,count,visitStatus,userId,userType
		}
		
		public String getFieldsColumnFamily(String field) {
			// next step is using hash method to instead of
			if (field == null)
				return null;
			for (int i = 0; i < m_logFields.length; i++) {
				for (int j = 1; j < m_logFields[i].length; j++) {
					if (field.compareTo((String) m_logFields[i][j]) == 0) {
						return m_logFields[i][0];
					}
				}
			}
			return null;
		}
		
		@Override
		public List<String>listFields() {
		// TODO Auto-generated method stub
			List<String> list = new ArrayList<String>();
			for(FIELDS fields:FIELDS.values()){
				list.add(fields.name());
			}
		return list;
		}
		
		public int getFieldsSize(){
			return FIELDS.values().length;
		}
		
		public int getPos(String str) {
			FIELDS field = FIELDS.valueOf(str);
			if (field != null)
				return field.ordinal();
			else
				return -1;
		}
}
