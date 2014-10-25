package com.tracker.hive.mapred;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

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

import com.tracker.common.constant.website.ReferrerType;
import com.tracker.common.data.ip.IPLocationReader;
import com.tracker.common.data.ip.LocationEntry;
import com.tracker.common.data.useragent.UserAgentUtil;
import com.tracker.common.log.ApachePVLog;
import com.tracker.common.utils.JsonUtil;

/**
 * web site 日志文件清洗
 * 
 * @author xiaorui.lu
 * 
 */
public class WebDataCleanedMR {
	static final Log LOG = LogFactory.getLog(WebDataCleanedMR.class);

	private static IPLocationReader ipReader;
	private static LocationEntry location;
	private static ArrayList<Object> list = new ArrayList<Object>();

	public static class DataCleanedMapper extends Mapper<Object, Text, NullWritable, Text> {
		private Text result = new Text();
		// 多路输出
		private MultipleOutputs<NullWritable, Text> multipleOutputs;

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
				ApachePVLog apachePVLog = JsonUtil.toObject(line, ApachePVLog.class);

				if (StringUtils.isNotBlank(apachePVLog.getWebId()) 
						&& StringUtils.isNotBlank(apachePVLog.getCookieId())
						&& StringUtils.isNotBlank(apachePVLog.getIp())) {
					
					long currentTime = System.currentTimeMillis();
					
					Long cookieCreateTime = apachePVLog.getCookieCreateTime();
					cookieCreateTime = (cookieCreateTime == null ? currentTime : cookieCreateTime);
					
					Long logTime = apachePVLog.getServerLogTime();
					logTime = (logTime == null ? currentTime : logTime);
					
					if (cookieCreateTime > currentTime || logTime > currentTime) {
						throw new IllegalArgumentException("cookieCreateTime or logTime is illegal");
					}
						
					Date date = new Date(logTime);
					DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
					String outDir = dateFormat.format(date);
					
					String ip = apachePVLog.getIp();
					String userAgent = apachePVLog.getUserAgent();
					location = ipReader.getLocationEntryByIp(ip);

					list.clear();
					list.add(apachePVLog.getWebId());
					list.add(logTime);
					list.add(apachePVLog.getVisitTime());
					list.add(apachePVLog.getUserId());
					list.add(apachePVLog.getUserType());
					list.add(apachePVLog.getCookieId());
					list.add(cookieCreateTime);
					list.add(ip);
					list.add(location.getCountry());
					list.add(location.getProvince());
					list.add(location.getCity());
					list.add(userAgent);
					list.add(UserAgentUtil.getBrowserByUserAgent(userAgent));
					list.add(UserAgentUtil.getOSByUserAgent(userAgent));
					list.add(apachePVLog.getColorDepth());
					list.add(apachePVLog.getIsCookieEnabled());
					
					String lang = apachePVLog.getLanguage();
					lang = (lang == null ? "" : lang.toLowerCase());
					list.add(lang);
					
					list.add(apachePVLog.getScreen());
					list.add(apachePVLog.getReferrer());
					list.add(apachePVLog.getCurUrl());
					list.add(apachePVLog.getTitle());
					list.add(apachePVLog.getRefDomain());
					list.add(apachePVLog.getRefSubDomin());
					list.add(apachePVLog.getRefKeyword());
					
					Integer refType = apachePVLog.getRefType();
					refType = (refType == null ? ReferrerType.DIRECT.getValue() : refType);
					list.add(refType);

					this.result.set(StringUtils.join(list, "`"));

					multipleOutputs.write(NullWritable.get(), this.result, outDir + "/");
				}
			} catch (Exception e) {
				LOG.debug(e.getMessage());
			}
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
			System.err.println("Usage: WebDataCleanedMR <in> <out>");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "KPI_MapReduce");
		job.setJarByClass(WebDataCleanedMR.class);
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
