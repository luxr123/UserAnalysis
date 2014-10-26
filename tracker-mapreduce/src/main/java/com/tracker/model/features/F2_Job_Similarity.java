package com.tracker.model.features;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class F2_Job_Similarity {

	public static final String COMMA = ","; // 逗号
	private static final String TABLE_DELIMETER = "\t"; // TABLE键
	public static final String A = "A";
	public static final String B = "B";

	public static class Step1_Map extends Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 获取输入文件的全路径和名称
			String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
			String line = value.toString();
			if (line == null || line.equals(""))
				return;
			String[] values = line.split(COMMA);
			if (values.length < 3)
				return;
			String rowindex = values[0];
			String colindex = values[1];
			String elevalue = values[2];
			context.write(new IntWritable(Integer.valueOf(colindex)), new Text("a#" + rowindex + "#" + elevalue));
			context.write(new IntWritable(Integer.valueOf(colindex)), new Text("b#" + rowindex + "#" + elevalue));
		}
	}

	public static class Step1_Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			Map<Integer, Integer> valAMap = new HashMap<Integer, Integer>();
			Map<Integer, Integer> valBMap = new HashMap<Integer, Integer>();
			for (Text v : values) {
				String value = v.toString();
				if (value.startsWith("a#")) {
					StringTokenizer token = new StringTokenizer(value, "#");
					String[] temp = new String[3];
					int k = 0;
					while (token.hasMoreTokens()) {
						temp[k] = token.nextToken();
						k++;
					}
					valAMap.put(Integer.parseInt(temp[1]), Integer.parseInt(temp[2]));
				} else if (value.startsWith("b#")) {
					StringTokenizer token = new StringTokenizer(value, "#");
					String[] temp = new String[3];
					int k = 0;
					while (token.hasMoreTokens()) {
						temp[k] = token.nextToken();
						k++;
					}
					valBMap.put(Integer.parseInt(temp[1]), Integer.parseInt(temp[2]));
				}
			}
			for (Entry<Integer, Integer> entryA : valAMap.entrySet())
				for (Entry<Integer, Integer> entryB : valBMap.entrySet()) {
					// out key = j, value = i \t k \t val
					int i = entryA.getKey();
					int k = entryB.getKey();
					if (i < k)// 对角线值为1，且为上三角矩阵
						context.write(key, new Text(i + TABLE_DELIMETER + k + TABLE_DELIMETER + entryA.getValue()
								+ TABLE_DELIMETER + entryB.getValue()));
				}
		}
	}

	public static class Step2_Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split(TABLE_DELIMETER);
			if (values.length == 5) {
				String i = values[1];
				String k = values[2];
				String valA = values[3];
				String valB = values[4];
				context.write(new Text(i + COMMA + k), new Text(valA + COMMA + valB));
			}
		}
	}

	public static class Step2_Reduce extends Reducer<Text, Text, Text, DoubleWritable> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			int mulV = 0;
			int squareA = 0;
			int squareB = 0;
			for (Text v : values) {
				String[] value = v.toString().split(COMMA);
				int valA = Integer.valueOf(value[0]);
				int valB = Integer.valueOf(value[1]);
				mulV += valA * valB;
				squareA += valA * valA;
				squareB += valB * valB;
			}
			double ret = mulV / (Math.sqrt(squareA) * Math.sqrt(squareB));
			context.write(key, new DoubleWritable(ret));
		}
	}

	public static void main(String[] args) throws Exception {
		String STEP1 = "/step1";
		String FINAL = "/final";
		Configuration conf = new Configuration();
		String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (remainingArgs.length != 2) {
			System.err.println("Usage: F2_Job_Similarity <in> <out>");
			System.exit(2);
		}
		Path in_1 = new Path(remainingArgs[0]);
		Path out_1 = new Path(remainingArgs[1] + STEP1);
		Path out_2 = new Path(remainingArgs[1] + FINAL);
		/** first stage */
		Job jobStep1 = new Job(conf, "F2_Job_Similarity_1");
		jobStep1.setMapperClass(Step1_Map.class);
		jobStep1.setReducerClass(Step1_Reduce.class);

		jobStep1.setOutputKeyClass(IntWritable.class);
		jobStep1.setOutputValueClass(Text.class);
		jobStep1.setInputFormatClass(TextInputFormat.class);
		jobStep1.setOutputFormatClass(TextOutputFormat.class);
		jobStep1.setNumReduceTasks(5);
		FileInputFormat.addInputPath(jobStep1, in_1);
		FileOutputFormat.setOutputPath(jobStep1, out_1);
		if (!jobStep1.waitForCompletion(true))
			System.exit(1);
		/** second stage */
		Job jobStep2 = new Job(conf, "F2_Job_Similarity_2");
		jobStep2.setMapperClass(Step2_Map.class);
		jobStep2.setReducerClass(Step2_Reduce.class);

		jobStep2.setOutputKeyClass(Text.class);
		jobStep2.setOutputValueClass(DoubleWritable.class);
		jobStep2.setInputFormatClass(TextInputFormat.class);
		jobStep2.setOutputFormatClass(TextOutputFormat.class);
		jobStep2.setNumReduceTasks(5);
		FileInputFormat.addInputPath(jobStep2, out_1);
		FileOutputFormat.setOutputPath(jobStep2, out_2);
		System.exit(jobStep2.waitForCompletion(true) ? 0 : 1);

	}
}
