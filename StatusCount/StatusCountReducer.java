package com.cloudwick.hadoop.statuscode;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/*
 * Created by Darshan S Mahendrakar
 * Date: 04/16/2015
 * Email: darshan.sm@cloudwick.com
 */
public class StatusCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable count = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		count.set(sum);
		context.write(key, count);

	}
}
