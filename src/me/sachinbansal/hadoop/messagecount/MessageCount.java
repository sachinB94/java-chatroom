package me.sachinbansal.hadoop.messagecount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MessageCount {
	
	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		
		private Text address = new Text();
		private final static LongWritable one = new LongWritable(1);
		
		@Override
		protected void map(LongWritable key, Text line, Context context)
				throws IOException, InterruptedException {
			String[] data = line.toString().split("\t");
			
			address.set(data[0]);
			context.write(address, one);
		}
	}
	
	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		private LongWritable result = new LongWritable(0);
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			
			while (values.iterator().hasNext()) {
				result.set(result.get() + 1);
				values.iterator().next();
			}
			context.write(key, result);
		}
	}

}
