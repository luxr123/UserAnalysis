package com.tracker.model.features;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.tracker.common.log.JobInfo;

public class F3_User_Similarity {

	public static final String COMMA = ","; // 逗号
	public static final String SHARP = "#"; // sharp
	private static final String TAB = "\t"; // TABLE键
	public static final String A = "A";
	public static final String B = "B";

	/**
	 * 职位类别相似度计算
	 * 
	 * <pre>
	 * 用户，类型，类型值，value,sqrtVal
	 * （1,1,1,0.333,0.024）
	 * （1,2,1,0.333,0.024）
	 * （1,3,1,0.333,0.024）
	 * （1,4,1,0.333,0.024）
	 * （1,5,1,0.333,0.024）
	 * （1,6,1,0.333,0.024）
	 * （1,7,1,0.333,0.024）
	 * （1,8,1,0.333,0.024）
	 * （1,9,1,0.333,0.024）
	 * </pre>
	 */
	public static class Step1_Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if (line == null || line.equals(""))
				return;
			String[] values = line.split(COMMA);
			if (values.length < 5)
				return;
			String userid = values[0];
			String type = values[1];
			String typeVal = values[2];
			String interVal = values[3];
			String sqrtValue = values[4];
			String k = type + COMMA + typeVal;
			userid += COMMA + type;
			context.write(new Text(k), new Text("a" + SHARP + userid + SHARP + interVal + SHARP + sqrtValue));
			context.write(new Text(k), new Text("b" + SHARP + userid + SHARP + interVal + SHARP + sqrtValue));
		}
	}

	/**
	 * <pre>
	 * (类型，类型值)j-0	(a#userid#interVal#sqrtValue)
	 * </pre>
	 */
	public static class Step1_Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, String[]> valAMap = new HashMap<String, String[]>();
			Map<String, String[]> valBMap = new HashMap<String, String[]>();
			for (Text v : values) {
				String[] arr = v.toString().split(SHARP);
				if (arr[0].equals("a"))
					valAMap.put(arr[1], new String[] { arr[2], arr[3] });
				else if (arr[0].equals("b"))
					valBMap.put(arr[1], new String[] { arr[2], arr[3] });
			}
			for (Entry<String, String[]> entryA : valAMap.entrySet())
				for (Entry<String, String[]> entryB : valBMap.entrySet()) {
					// out key = j, value = i \t k \t val
					String i = entryA.getKey();// u1-0
					String k = entryB.getKey();// u2-0
					if (i.compareTo(k) < 0)// 对角线值为1，且为上三角矩阵
						context.write(key, new Text(i + TAB + k + TAB + entryA.getValue()[0] + TAB + entryA.getValue()[1] + TAB
								+ entryB.getValue()[0] + TAB + entryB.getValue()[1]));
				}
		}
	}

	/**
	 * j-0 u1-0 u2-0 u1-j-interV u1-interV u2-j-interV u2-interV
	 */
	public static class Step2_Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split(TAB);
			if (values.length == 7) {
				String i = values[1];
				String k = values[2];
				String valA = values[3];
				String squareA = values[4];
				String valB = values[5];
				String squareB = values[6];

				String[] userid_type = i.split(COMMA);
				String rowkey = userid_type[0] + TAB + k.split(COMMA)[0] + TAB + userid_type[1];// u1 u2 0
				context.write(new Text(rowkey), new Text(valA + COMMA + squareA + COMMA + valB + COMMA + squareB));
			}
		}
	}

	public static class Step2_Reduce extends Reducer<Text, Text, Text, DoubleWritable> {

		double squareA, squareB;
		boolean first = true;

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double mulV = 0;
			for (Text v : values) {
				String[] value = v.toString().split(COMMA);
				double valA = Double.valueOf(value[0]);
				double valB = Double.valueOf(value[2]);
				if (first) {
					squareA = Double.valueOf(value[1]);
					squareB = Double.valueOf(value[3]);
					first = false;
				}
				mulV += valA * valB;
			}
			double ret = mulV / (Math.sqrt(squareA) * Math.sqrt(squareB));
			context.write(key, new DoubleWritable(ret));
		}
	}

	/**
	 * u1 u2 0 0.03
	 */
	public static class Step3_Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split(TAB);
			if (values.length == 4) {
				String u1 = values[0];
				String u2 = values[1];
				String type = values[2];
				String simVal = values[3];

				context.write(new Text(u1 + COMMA + u2), new Text(type + COMMA + simVal));
			}
		}
	}

	public static class Step3_Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
		Map<Integer, Double> weight;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			weight = new HashMap<Integer, Double>() {
				{
					put(JobInfo.CATEGORY, 0.0325);
					put(JobInfo.PROPERTY, 0.0284);
					put(JobInfo.WORKPLACE, 0.2017);
					put(JobInfo.WORKYEAR, 0.0991);
					put(JobInfo.PROFESSIONAL, 0.0789);
					put(JobInfo.SALARY, 0.0849);
					put(JobInfo.EDUCATION, 0.1145);
					put(JobInfo.AGE, 0.0830);
					put(JobInfo.GENDER, 0.2770);
				}
			};
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			for (Text v : values) {
				String[] value = v.toString().split(COMMA);
				int type = Integer.valueOf(value[0]);
				double simVal = Double.valueOf(value[1]);
				sum += weight.get(type) * simVal;
			}
			if (sum >= 0.03)
				context.write(key, new DoubleWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		String STEP1 = "/step1";
		String STEP2 = "/step2";
		String FINAL = "/final";
		Configuration conf = new Configuration();
		String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (remainingArgs.length != 2) {
			System.err.println("Usage: F2_Job_Similarity <in> <out>");
			System.exit(2);
		}
		Path in_1 = new Path(remainingArgs[0]);
		Path out_1 = new Path(remainingArgs[1] + STEP1);
		Path out_2 = new Path(remainingArgs[1] + STEP2);
		Path out_3 = new Path(remainingArgs[1] + FINAL);
		/** first stage */
		Job jobStep1 = new Job(conf, "F3_User_Similarity_1");
		jobStep1.setJarByClass(F3_User_Similarity.class);
		jobStep1.setMapperClass(Step1_Map.class);
		jobStep1.setReducerClass(Step1_Reduce.class);

		jobStep1.setOutputKeyClass(Text.class);
		jobStep1.setOutputValueClass(Text.class);
		jobStep1.setInputFormatClass(TextInputFormat.class);
		jobStep1.setOutputFormatClass(TextOutputFormat.class);
		jobStep1.setNumReduceTasks(5);
		FileInputFormat.addInputPath(jobStep1, in_1);
		FileOutputFormat.setOutputPath(jobStep1, out_1);
		if (!jobStep1.waitForCompletion(true))
			System.exit(1);
		/** second stage */
		Job jobStep2 = new Job(conf, "F3_User_Similarity_2");
		jobStep2.setJarByClass(F3_User_Similarity.class);
		jobStep2.setMapperClass(Step2_Map.class);
		jobStep2.setReducerClass(Step2_Reduce.class);

		jobStep2.setMapOutputKeyClass(Text.class);
		jobStep2.setMapOutputValueClass(Text.class);
		jobStep2.setOutputKeyClass(Text.class);
		jobStep2.setOutputValueClass(DoubleWritable.class);
		jobStep2.setInputFormatClass(TextInputFormat.class);
		jobStep2.setOutputFormatClass(TextOutputFormat.class);
		jobStep2.setNumReduceTasks(5);
		FileInputFormat.addInputPath(jobStep2, out_1);
		FileOutputFormat.setOutputPath(jobStep2, out_2);
		if (!jobStep2.waitForCompletion(true))
			System.exit(1);
		/** third stage */
		Job jobStep3 = new Job(conf, "F3_User_Similarity_final");
		jobStep3.setJarByClass(F3_User_Similarity.class);
		jobStep3.setMapperClass(Step3_Map.class);
		jobStep3.setReducerClass(Step3_Reduce.class);

		jobStep3.setMapOutputKeyClass(Text.class);
		jobStep3.setMapOutputValueClass(Text.class);
		jobStep3.setOutputKeyClass(Text.class);
		jobStep3.setOutputValueClass(DoubleWritable.class);
		jobStep3.setInputFormatClass(TextInputFormat.class);
		jobStep3.setOutputFormatClass(TextOutputFormat.class);
		jobStep3.setNumReduceTasks(5);
		FileInputFormat.addInputPath(jobStep3, out_2);
		FileOutputFormat.setOutputPath(jobStep3, out_3);
		System.exit(jobStep3.waitForCompletion(true) ? 0 : 1);
	}
}
