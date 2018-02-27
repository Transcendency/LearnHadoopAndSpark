package Transcendency.com.github.WC;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {
	public static void main(String[] args) throws Exception{
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperature <input path> <output path>");
			System.exit(-1);
		}
		
		// OLD MAP REDUCE API
//		JobConf conf = new JobConf(MaxTemperature.class); conf.setJobName("Max temperature");
//		FileInputFormat.addInputPath(conf, new Path(args[0])); 
//		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
//		conf.setMapperClass(MaxTemperatureMapper.class); 
//		conf.setReducerClass(MaxTemperatureReducer.class);
//		conf.setOutputKeyClass(Text.class); 
//		conf.setOutputValueClass(IntWritable.class);
//		try {
//			JobClient.runJob(conf);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		// NEW MAP REDUCE API
		Job job = new Job();
		job.setJarByClass(MaxTemperature.class);
		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(NewMaxTemperatureMapper.class);
		job.setReducerClass(NewMaxTemperatureReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
