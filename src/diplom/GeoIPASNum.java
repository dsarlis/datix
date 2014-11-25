package diplom;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class GeoIPASNum {
	 public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
		        
		    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		        String line = value.toString();
		        String[] fields = line.split(",");
		        long ipStart = Long.parseLong(fields[0]);
		        long ipEnd = Long.parseLong(fields[1]); 
		        String as = fields[2].replace("\"", "");
		        long i = 0;
		        
		        for (i=ipStart; i <= ipEnd; i++)
		        	context.write(new LongWritable(i), new Text(as));
		    }
		 }
		        
		        
		 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		        
		    Job job = new Job(conf, "geoipasnum");
		    
		    job.setJarByClass(GeoIPASNum.class);
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
