package com.cloudwick.hadoop.statuscode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Created by Darshan S Mahendrakar
 * Date: 06/05/2015
 * Email: darshan.sm@cloudwick.com
 */
public class StatusCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text statusCode = new Text();
	Pattern pattern = Pattern.compile("(?<=\"\\s)\\d{3}(?=\\s+\\d)");
	Matcher matcher;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		matcher = pattern.matcher(line);
		matcher.find();
		statusCode.set(matcher.group());
		context.write(statusCode, one);
	}
}
