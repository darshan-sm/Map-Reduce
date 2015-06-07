package com.cloudwick.hadoop.statuscode;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Created by Darshan S Mahendrakar
 * Date: 06/05/2015
 * Email: darshan.sm@cloudwick.com
 */
public class StatusCountDriver extends Configured implements Tool {

	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage:StatusCountDriver <in> <out>");
			System.exit(2);
		}
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("status count");
		job.setJarByClass(StatusCountDriver.class);
		job.setMapperClass(StatusCountMapper.class);
		job.setCombinerClass(StatusCountReducer.class);
		job.setReducerClass(StatusCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int ret = job.waitForCompletion(true) ? 0 : 1;
		return ret;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new StatusCountDriver(),
				args);
	}

}
