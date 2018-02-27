package Transcendency.com.github.WC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NewMaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private static final int MISSING = 9999;
	public void map(LongWritable key, Text value, Context context) {
		String line = value.toString();
		String year = line.substring(15, 19);
		int airTemperature;
		if (line.charAt(87) == '+') {
			// parse doesn't like leading plus sign
			airTemperature = Integer.parseInt(line.substring(88, 92));
		}else {
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		String quality = line.substring(92, 93);
		if (airTemperature != MISSING && quality.matches("[01459]")) {
			try {
				context.write(new Text(year), new IntWritable(airTemperature));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
