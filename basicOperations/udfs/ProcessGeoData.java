package diplom;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class ProcessGeoData {
	public static class MapAS extends Mapper<LongWritable, Text, Text, Text> {
		static int k = 0;
		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	k++;
	    	if (k < 1000) {
	    		String line = value.toString();
	    		String[] fields = line.split(",");
	    		long ipStart = Long.parseLong(fields[0]);
	    		long ipEnd = Long.parseLong(fields[1]); 
	    		String as = fields[2];
	    		long i = 0;
	        
	        	for (i=ipStart; i <= ipEnd; i++)
	        		context.write(new Text(""+i), new Text(as+"_"+"as"));
	    	}
	    }
	 }
	
	public static class MapCountry extends Mapper<LongWritable, Text, Text, Text> {
		static int k = 0;
		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	k++;
	    	if (k < 1000) {
	    		String line = value.toString();
	        	String[] fields = line.split(",");
	        	long ipStart = Long.parseLong(fields[2].replace("\"", ""));
	        	long ipEnd = Long.parseLong(fields[3].replace("\"", "")); 
	        	String countryCode = fields[4];
	        	String countryName = fields[5];
	        	long i = 0;
	        
	        	for (i=ipStart; i <= ipEnd; i++)
	        		context.write(new Text(""+i), new Text(countryCode + "," + countryName + "_" + "co"));
	    	}
	    }
	 }
	
	public static class MapBlocks extends Mapper<LongWritable, Text, Text, Text> {
		static int k = 0;
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	k++;
	    	if (k< 1000) {
	    		String line = value.toString();
	    		String[] fields = line.split(",");
	    		long ipStart = Long.parseLong(fields[0].replace("\"", ""));
	    		long ipEnd = Long.parseLong(fields[1].replace("\"", "")); 
	    		String locid = fields[2].replace("\"", "");
	    		long i = 0;
	        
	        	for (i=ipStart; i <= ipEnd; i++)
	        		context.write(new Text(""+i), new Text(locid + "_" + "lid"));
	    	}
	    }
	 }
	  
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

	    public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	    	String result = key + ",";
	    	for (Text value : values) {
	    		String[] fields = value.toString().split("_");
	    		if (fields[1].equals("as")) {
	    			result += fields[0] + ",";
	    			break;
	    		}
	    	}
	    	
	    	for (Text value : values) {
	    		String[] fields = value.toString().split("_");
	    		if (fields[1].equals("co")) {
	    			result += fields[0] + ",";
	    			break;
	    		}
	    	}
	    	
	    	for (Text value : values) {
	    		String[] fields = value.toString().split("_");
	    		if (fields[1].equals("lid")) {
	    			result += fields[0];
	    			break;
	    		}
	    	}
	    	
	    	context.write(new Text(result), null);
	    }
	 } 
	        
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	    Job job = new Job(conf, "processgeodata");
	    
	    job.setJarByClass(ProcessGeoData.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setMapperClass(MapAS.class);
	    job.setMapperClass(MapCountry.class);
	    job.setMapperClass(MapBlocks.class);
	    job.setReducerClass(Reduce.class);
	    
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    TextOutputFormat.setCompressOutput(job, true);
	    TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	        
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapAS.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapCountry.class);
	    MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, MapBlocks.class);
	    
	    FileOutputFormat.setOutputPath(job, new Path(args[3]));
	        
	    job.waitForCompletion(true);
	 }
}
