package com.tracker.hive.mapred;

import java.io.IOException;

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

import com.tracker.common.log.ApachePVLog;
import com.tracker.common.log.ApacheSearchLog;
import com.tracker.common.log.parser.ApacheLogParser;
import com.tracker.common.log.parser.LogParser;
import com.tracker.common.log.parser.LogParser.LogResult;
import com.tracker.hive.Constants;

/**
 * apache log日志文件清洗, 包括apachePVLog 和 searchLog日志文件分离
 * 
 * @author xiaorui.lu
 * 
 */
public class ApacheLogCleaned {
	static final Log LOG = LogFactory.getLog(ApacheLogCleaned.class);

	public static class ApacheLogMapper extends Mapper<Object, Text, NullWritable, Text> {
		// 多路输出
		private MultipleOutputs<NullWritable, Text> multipleOutputs;
		private LogParser logParser;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
			logParser = new ApacheLogParser();
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String outDir = Constants.APACHElOG;
				
				LogResult result = logParser.parseLog(value.toString());
				if (result == null) {
					return;
				}
					
				String logType = result.getLogTypeMappring();
				if (logType.equals(ApacheSearchLog.APACHE_SEARCH_LOG_TYPE)) {
					outDir = Constants.APACHESEARCHLOG;
				} else if (logType.equals(ApachePVLog.APACHE_PV_LOG_TYPE)) {
					outDir = Constants.APACHEPVLOG;
				}
					
				String logJson = result.getLogJson();
				multipleOutputs.write(NullWritable.get(), new Text(logJson), outDir + "/");
			} catch (Exception e) {
				LOG.error("error to parse log:" + value.toString(), e);
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
			System.err.println("Usage: ApacheLogCleaned <in> <out>");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "ApacheLogCleaned");
		
		job.setJarByClass(ApacheLogCleaned.class);
		job.setMapperClass(ApacheLogMapper.class);
		
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
