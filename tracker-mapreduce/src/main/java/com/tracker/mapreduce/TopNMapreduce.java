package com.tracker.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapTask.MapOutputBuffer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.task.reduce.MapOutput.MapOutputComparator;

public class TopNMapreduce {
	public static final int MAXRETURN = 1000;

	public static class TopNMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			// zookeeper?
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		}
	}

	public static class TopNReduce extends Reducer<Text, Text, Text, Text> {

		private Iterable<Integer> arg1;

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		}
	}

	public static class TopNCombine extends Reducer<Text, Text, Text, Text> {

		private Iterable<Text> arg1;

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (Text text : arg1) {
				context.write(arg0, text);
			}
		}

		@Override
		public void run(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			setup(context);
			int count = 0;
			try {
				while (context.nextKey()) {
					if (count++ < MAXRETURN)
						reduce(context.getCurrentKey(), context.getValues(),
								context);
					// If a back up store is used, reset it
					Iterator<Text> iter = context.getValues().iterator();
					if (iter instanceof ReduceContext.ValueIterator) {
						((ReduceContext.ValueIterator<Text>) iter)
								.resetBackupStore();
					}
				}
			} finally {
				cleanup(context);
			}
		}

		// for desc sort function
	}

	public static class DescMapOutputBuffer extends MapOutputBuffer<Text, Text> {
		public int compare(final int mi, final int mj) {
			switch (super.compare(mi, mj)) {
			case -1:
				return 1;
			case 0:
				return 0;
			case 1:
				return -1;
			default:
				return 0;
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
