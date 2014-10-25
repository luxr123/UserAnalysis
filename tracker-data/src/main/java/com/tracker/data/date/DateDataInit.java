package com.tracker.data.date;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Calendar;
import java.util.GregorianCalendar;

import com.tracker.common.utils.StringUtil;

public class DateDataInit {
	private BufferedWriter bw = null;
	public int index  = 1;
	
	public DateDataInit() throws IOException{
		bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("d_date.txt"), "utf-8"));
	}
	
	public void showYear(int year) throws IOException{
		Calendar cal=Calendar.getInstance();
		cal.setFirstDayOfWeek(GregorianCalendar.MONDAY);//每周以周一开始
		cal.setMinimalDaysInFirstWeek(3);//每年的第一周必须大于或等于3天，否则就算上一年的最后一周
		cal.set(year, 0, 1);//month从0开始
		
		while(cal.get(Calendar.YEAR) == year){
			int month = cal.get(Calendar.MONTH) + 1;
			int day =  cal.get(Calendar.DAY_OF_MONTH);
			StringBuffer sb = new StringBuffer();
			sb.append(index++).append("\t"); //主键
			sb.append(year  + fillDate(month)  + fillDate(day)).append("\t");// 日期（YYYYMMDD）
			sb.append(year  + fillDate(month)).append("\t");//日历（YYYYMM）
			sb.append(year).append("\t"); //年份
			sb.append(month).append("\t"); // 月份(1 -12)
			sb.append(cal.get(Calendar.DAY_OF_MONTH)).append("\t");//月中第几天
			sb.append(getQuarter(month)).append("\t");//季度(1,2,3,4)
			sb.append(getCurrentWeekOfMonth(cal)).append("\t");//星期几（1 – 7）
			int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);
			if(month == 12 && weekOfYear == 1){
				sb.append((year+1) + StringUtil.fillLeftData(String.valueOf(weekOfYear), 2, '0')).append("\t");//年中第几周
			} else {
				sb.append(year + StringUtil.fillLeftData(String.valueOf(weekOfYear), 2, '0')).append("\t");//年中第几周
			}
			
			int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
			//星期指示符， (周末1，工作日0)
			if(dayOfWeek == 1 || dayOfWeek == 7){
				sb.append("1");
			} else {
				sb.append("0");
			}
			bw.write(sb.toString() + "\n");
			cal.setTimeInMillis(cal.getTimeInMillis() + 24*60*60*1000);
		}
		bw.flush();
	}
	
	public String fillDate(int date){
		if(date < 10)
			return "0" + date;
		else 
			return date + "";
	}
	
	public void printOthers() throws IOException{
		bw.write(index++ + "\t" + 2 + "\t  \t  \t  \t  \t  \t  \t \t \t \t \n");
		bw.write(index++ + "\t" + 3 + "\t  \t  \t  \t  \t  \t  \t \t \t \t \n");
		bw.write(index++ + "\t" + 4 + "\t  \t  \t  \t  \t  \t  \t \t \t \t \n");
	}
	
	
	
	 public  String getCurrentWeekOfMonth(Calendar calendar) {
		  String strWeek = "";
		  int dw = calendar.get(Calendar.DAY_OF_WEEK);
		  if (dw == 1) {
		   strWeek = "7";
		  } else if (dw == 2) {
		   strWeek = "1";
		  } else if (dw == 3) {
		   strWeek = "2";
		  } else if (dw == 4) {
		   strWeek = "3";
		  } else if (dw == 5) {
		   strWeek = "4";
		  } else if (dw == 6) {
		   strWeek = "5";
		  } else if (dw == 7) {
		   strWeek = "6";
		  }
		  return strWeek;
		 }
	
	/**
	 * 获取季度
	 * @param month
	 * @return
	 */
	public int getQuarter(int month){
		if(month <= 3){
			return 1;
		} else if(month <= 6)
			return 2;
		else if(month <= 9)
			return 3;
		else 
			return 4;
	}	
	
	public void close() throws IOException{
		if(bw != null ){
			bw.close();
		}
	}
	
	public static void main(String[] args) throws IOException {
		DateDataInit dateInit = new DateDataInit();
		
		for(int year = 2014; year <= 2024; year++){
			dateInit.showYear(year);
		}
		dateInit.close();
		
//		Calendar cal=Calendar.getInstance();
//		cal.setFirstDayOfWeek(GregorianCalendar.MONDAY);//每周以周一开始
//		cal.setMinimalDaysInFirstWeek(3);//每年的第一周必须大于或等于3天，否则就算上一年的最后一周
//		cal.set(2015, 11,28);//month从0开始
//		
//		System.out.println(cal.get(Calendar.WEEK_OF_YEAR));
//		System.out.println(cal.get(Calendar.MONTH) + 1);
	}
}
