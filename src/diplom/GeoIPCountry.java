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


public class GeoIPCountry {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] fields = line.split(",");
	        long ipStart = Long.parseLong(fields[2].replace("\"", ""));
	        long ipEnd = Long.parseLong(fields[3].replace("\"", "")); 
	        String countryCode = fields[4].replace("\"", "");
	        String countryName = fields[4].replace("\"", "");
	        long i = 0;
	        
	        for (i=ipStart; i <= ipEnd; i++)
	        	context.write(new Text(""+i), new Text(countryCode + "\t" + countryName));
	    }
	 }
	        
	        
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	    Job job = new Job(conf, "geoipcountry");
	    
	    job.setJarByClass(GeoIPCountry.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reducer.class);
	    
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    TextOutputFormat.setCompressOutput(job, true);
	    TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	 }
}
