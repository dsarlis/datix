package diplom;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CreateIpAS {
	public static class Map extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	    ImmutableBytesWritable hkey = new ImmutableBytesWritable();
	        
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] fields = line.split(",");
	        
	        
	        hkey.set(fields[0].getBytes());
	        KeyValue kv = new KeyValue(hkey.get(), "data".getBytes(Charset.forName("UTF-8")), "AS".getBytes(Charset.forName("UTF-8")), 
	        						   fields[2].getBytes(Charset.forName("UTF-8")));
	        
	        context.write(hkey, kv);
	        kv = new KeyValue(hkey.get(), "data".getBytes(Charset.forName("UTF-8")), "borderIPofAS".getBytes(Charset.forName("UTF-8")), 
					   "start".getBytes(Charset.forName("UTF-8")));
	        context.write(hkey, kv);
	        
	        
	        hkey.set(fields[1].getBytes());
	        kv = new KeyValue(hkey.get(), "data".getBytes(Charset.forName("UTF-8")), "AS".getBytes(Charset.forName("UTF-8")), 
					   fields[2].getBytes(Charset.forName("UTF-8")));
	        context.write(hkey,kv);
	        kv = new KeyValue(hkey.get(), "data".getBytes(Charset.forName("UTF-8")), "borderIPofAS".getBytes(Charset.forName("UTF-8")), 
					   "stop".getBytes(Charset.forName("UTF-8")));
	        context.write(hkey, kv);
	    }
	 }
	        
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("hbase.table.name", args[2]);    
	    
	    Job job = new Job(conf, "createipas");
	    
	    job.setJarByClass(CreateIpAS.class);
	    job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(KeyValue.class);
	    
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reducer.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(HFileOutputFormat.class);
	    
	    HTable hTable = new HTable(conf, args[2]);
	    HFileOutputFormat.configureIncrementalLoad(job, hTable);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	 }
}
