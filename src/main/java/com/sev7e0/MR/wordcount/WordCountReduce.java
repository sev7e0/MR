package com.sev7e0.MR.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
	/**
	 * 
	 */
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		Long count = 0L;
		for (LongWritable value : values) {
			count += value.get();
		}
		//统计结果的输出
		context.write(key, new LongWritable(count));
	}
}
