package me.sachinbansal.hadoop.analysis;

import java.util.Date;

import me.sachinbansal.hadoop.analysis.MessageCount.Map;
import me.sachinbansal.hadoop.analysis.MessageCount.Reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Analysis {
	public void countMessage(Date time) throws Exception {
		String datetime = time.toString().replaceAll(" ", "_").replaceAll(":", "-");
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJobName("messagecount");
		job.setJar("WebContent/WEB-INF/lib/messagecount.jar");
		
	    job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/user/luffy/chatroom/" + datetime + "/part-m-00000"));
		job.setInputFormatClass(TextInputFormat.class);
	
		FileOutputFormat.setOutputPath(job, new Path("/user/luffy/chatroom-analysis/" + datetime));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
	}
}