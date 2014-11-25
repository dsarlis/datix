package diplom;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LoadDnsHive {
	public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
	        
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] fields = line.split(",");
	        
	        if (fields.length == 2) { 
	        	String[] ipParts = fields[0].split("\\.");
	        	long intIP = Long.parseLong(ipParts[0]) * 16777216 + Long.parseLong(ipParts[1]) * 65536 
       				 + Long.parseLong(ipParts[2]) * 256 + Long.parseLong(ipParts[3]);
	        
	        	context.write(new LongWritable(intIP), new Text(fields[1]));
	        }
	    }
	 }
	        
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();    
	    
	    Job job = new Job(conf, "loadipdns");
	    
	    job.setJarByClass(LoadIPDns.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reducer.class);
	        
	    job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setNumReduceTasks(100);
	    
	    TextOutputFormat.setCompressOutput(job, true);
	    TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);	    
	 }
}
