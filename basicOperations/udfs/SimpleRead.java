package diplom;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class SimpleRead {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	        
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    }
	 } 
	        
	 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
	    }
	 }
	        
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	    Job job = new Job(conf, "simpleread");
	    
	    job.setJarByClass(SimpleRead.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	 }
	
}
