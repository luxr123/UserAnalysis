package com.tracker.hive.mapred;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.tracker.common.data.ip.IPLocationReader;
import com.tracker.common.data.ip.LocationEntry;
import com.tracker.common.data.useragent.UserAgentUtil;
import com.tracker.common.log.ApacheSearchLog;
import com.tracker.common.log.condition.CaseSearchCondition;
import com.tracker.common.log.condition.ManagerSearchCondition;
import com.tracker.common.utils.JsonUtil;

/**
 * searchLog 日志文件清洗
 * 
 * @author xiaorui.lu
 * 
 */
public class SearchDataCleanedMR {
	static final Log LOG = LogFactory.getLog(SearchDataCleanedMR.class);

	private static IPLocationReader ipReader;
	private static LocationEntry location;
	private static ArrayList<Object> apacheSearchList = new ArrayList<Object>();
	private static ArrayList<Object> apacheWebList = new ArrayList<Object>();
	private static ArrayList<Object> conditionList = new ArrayList<Object>();

	public static class DataCleanedMapper extends Mapper<Object, Text, NullWritable, Text> {
		// 多路输出
		private MultipleOutputs<NullWritable, Text> multipleOutputs;
		
		private Text apacheSearchResult = new Text();
		private Text apacheWebResult = new Text();
		private Text conditionResult = new Text();


		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
			// 加载distributed cache file
			ipReader = new IPLocationReader("qqwry.dat", "universityLocation.txt");
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			try {
				ApacheSearchLog searchLog = JsonUtil.toObject(line, ApacheSearchLog.class);

				if (StringUtils.isNotBlank(searchLog.getWebId()) 
						&& StringUtils.isNotBlank(searchLog.getCurUrl())
						&& StringUtils.isNotBlank(searchLog.getCategory()) 
						&& searchLog.getSearchType() != null
						&& StringUtils.isNotBlank(searchLog.getCookieId())) {
					
					long currentTime = System.currentTimeMillis();
					
					Long cookieCreateTime = searchLog.getCookieCreateTime();
					cookieCreateTime = (cookieCreateTime == null ? currentTime : cookieCreateTime);
					
					Long logTime = searchLog.getServerLogTime();
					logTime = (logTime == null ? currentTime : logTime);
					
					
					if (cookieCreateTime > currentTime || logTime > currentTime) {
						throw new IllegalArgumentException("cookieCreateTime or logTime is illegal");
					} 
                    
					Date date = new Date(logTime);
					DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
					String outDir = dateFormat.format(date);
					
					// apache search
					String conditionId = initApacheSearchFields(searchLog, cookieCreateTime, logTime);
					this.apacheSearchResult.set(StringUtils.join(apacheSearchList, "`"));
					multipleOutputs.write(NullWritable.get(), this.apacheSearchResult, outDir + "/apache_search/");
  
					// apache web
					initApacheWebFields(searchLog, cookieCreateTime, logTime);
					this.apacheWebResult.set(StringUtils.join(apacheWebList, "`"));
					multipleOutputs.write(NullWritable.get(), this.apacheWebResult, outDir + "/apache_web/");
					
					// 搜索条件
					if (searchLog.getCategory().equalsIgnoreCase(ManagerSearchCondition.SEARCH_ENGIN_NAME)) {
						ManagerSearchCondition searchCondition = JsonUtil.toObject(searchLog.getSearchConditionJson(), ManagerSearchCondition.class);
						initManagerConditionFields(conditionId, searchCondition);
						
						this.conditionResult.set(StringUtils.join(conditionList, "`"));
						
						multipleOutputs.write(NullWritable.get(), this.conditionResult, outDir + "/manager_search/");
					} else if (searchLog.getCategory().equalsIgnoreCase(CaseSearchCondition.SEARCH_ENGIN_NAME)) {
						CaseSearchCondition searchCondition = JsonUtil.toObject(searchLog.getSearchConditionJson(), CaseSearchCondition.class);
						initCaseConditionFields(conditionId, searchCondition);
						
						this.conditionResult.set(StringUtils.join(conditionList, "`"));
						
						multipleOutputs.write(NullWritable.get(), this.conditionResult, outDir + "/case_search/");
					}
				} else {
					multipleOutputs.write(NullWritable.get(), new Text(), "other1/");
				}
			} catch (Exception e) {
				multipleOutputs.write(NullWritable.get(), new Text(), "other2/");
				LOG.debug(e.getMessage());
			}
		}

		/**
		 * 初始化case搜索条件的字段
		 * @param conditionId
		 * @param searchCondition
		 */
		private void initCaseConditionFields(String conditionId, CaseSearchCondition searchCondition) {
			conditionList.clear();
			conditionList.add(conditionId);
			conditionList.add(searchCondition.getAlltext());
			conditionList.add(searchCondition.getArea());
			conditionList.add(searchCondition.getIndustry());
			conditionList.add(searchCondition.getPay());
			conditionList.add(searchCondition.getCommission());
			conditionList.add(searchCondition.getPrepaid());
			conditionList.add(searchCondition.getExclusive());
//			conditionList.add(searchCondition.getIsHRCase());
			conditionList.add(searchCondition.getSpyKeywords());
			conditionList.add(searchCondition.getCaseName());
		}

		/**
		 * 初始化manager搜索条件的字段
		 * @param conditionId
		 * @param searchCondition
		 */
		private void initManagerConditionFields(String conditionId, ManagerSearchCondition searchCondition) {
			conditionList.clear();
			conditionList.add(conditionId);
			conditionList.add(searchCondition.getDiv());
			conditionList.add(searchCondition.getCompany());
			conditionList.add(searchCondition.getNiscohis());
			conditionList.add(searchCondition.getArea());
			conditionList.add(searchCondition.getFullText());
			conditionList.add(searchCondition.getNisseniordb());
			conditionList.add(searchCondition.getCompanyType());
			conditionList.add(searchCondition.getCompanySize());
			conditionList.add(searchCondition.getCorePos());
			conditionList.add(searchCondition.getDegree());
			conditionList.add(searchCondition.getIndustry());
			conditionList.add(searchCondition.getPosLevel());
			conditionList.add(searchCondition.getSex());
			conditionList.add(searchCondition.getWorkYear());
			conditionList.add(searchCondition.getCompanyText());
			conditionList.add(searchCondition.getPosText());
		}

		/**
		 * 初始化搜索条件的字段
		 * @param searchLog
		 * @param cookieCreateTime
		 * @param logTime
		 * @return
		 */
		private String initApacheSearchFields(ApacheSearchLog searchLog, Long cookieCreateTime, Long logTime) {
			apacheSearchList.clear();
			apacheSearchList.add(searchLog.getServerIp());
			apacheSearchList.add(logTime);
			apacheSearchList.add(searchLog.getVisitStatus());
			apacheSearchList.add(searchLog.getUserAgent());
			apacheSearchList.add(searchLog.getWebId());
			apacheSearchList.add(searchLog.getCurUrl());
			apacheSearchList.add(searchLog.getCategory());
			apacheSearchList.add(searchLog.getVisitTime());
			apacheSearchList.add(searchLog.getUserId());
			apacheSearchList.add(searchLog.getUserType());
			apacheSearchList.add(searchLog.getCookieId());
			apacheSearchList.add(cookieCreateTime);
			apacheSearchList.add(searchLog.getIp());
			apacheSearchList.add(searchLog.getResponseTime());
			apacheSearchList.add(searchLog.getTotalCount());
			apacheSearchList.add(searchLog.getResultCount());
			apacheSearchList.add(searchLog.getCurPageNum());
			apacheSearchList.add(searchLog.getSearchType());
			apacheSearchList.add(searchLog.getSearchShowType());
			apacheSearchList.add(searchLog.getSearchParam());
			Boolean isCallSe = searchLog.getIsCallSE();
			if (null == isCallSe)
				isCallSe = false;
			apacheSearchList.add(isCallSe);

			String conditionId = UUID.randomUUID().toString();
			apacheSearchList.add(conditionId);
			return conditionId;
		}

		/**
		 * 初始化website字段
		 * @param searchLog
		 * @param cookieCreateTime
		 * @param logTime
		 */
		private void initApacheWebFields(ApacheSearchLog searchLog, Long cookieCreateTime, Long logTime) {
			apacheWebList.clear();
			apacheWebList.add(searchLog.getWebId());
			apacheWebList.add(logTime);
			apacheWebList.add(searchLog.getVisitTime());
			apacheWebList.add(searchLog.getUserId());
			apacheWebList.add(searchLog.getUserType());
			apacheWebList.add(searchLog.getCookieId());
			apacheWebList.add(cookieCreateTime);
			String ip = searchLog.getIp();
			location = ipReader.getLocationEntryByIp(ip);
			apacheWebList.add(ip);
			apacheWebList.add(location.getCountry());
			apacheWebList.add(location.getProvince());
			apacheWebList.add(location.getCity());
			String userAgent = searchLog.getUserAgent();
			apacheWebList.add(userAgent);
			apacheWebList.add(UserAgentUtil.getBrowserByUserAgent(userAgent));
			apacheWebList.add(UserAgentUtil.getOSByUserAgent(userAgent));
			apacheWebList.add(searchLog.getColorDepth());
			apacheWebList.add(searchLog.getIsCookieEnabled());
			String lang = searchLog.getLanguage();
			lang = (null == lang ? "" : lang.toLowerCase());
			apacheWebList.add(lang);
			apacheWebList.add(searchLog.getScreen());
			apacheWebList.add("");
			apacheWebList.add(searchLog.getCurUrl());
			apacheWebList.add("");
			apacheWebList.add("");
			apacheWebList.add("");
			apacheWebList.add("");
			apacheWebList.add(searchLog.getRefType());
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (remainingArgs.length != 2) {
			System.err.println("Usage: SearchDataCleanedMR <in> <out>");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Search_MapReduce");
		job.setJarByClass(SearchDataCleanedMR.class);
		job.setMapperClass(DataCleanedMapper.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		//输入
		for (String path : remainingArgs[0].split(",")) {
			FileInputFormat.addInputPath(job, new Path(path));
		}
		//输出
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

		job.setNumReduceTasks(0);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
