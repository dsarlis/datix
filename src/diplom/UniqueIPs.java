package diplom;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.TreeMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class UniqueIPs {
 public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
	 	private TreeMap<String, Integer> IPMap = new TreeMap<String, Integer>();
	 	private int partition_no = 148;
	 
	 	public void setup(Context context) throws IOException{
			Path[] uris = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			BufferedReader br = new BufferedReader(new FileReader(uris[0].toString()));
			try {
			     String line = br.readLine();
			     
			     int id = 0;
			     while (line != null) {
			         IPMap.put(line, id);
			         id++;
			         line = br.readLine();
			     }
			     
			    // partition_no = id + 2;
			} catch (IOException e) {
				System.out.println("Sorry, not a valid file location.");
			} finally {
			      br.close();
			}
		}
	 
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split(" ");
            
            String ipFrom = values[0];
            String ipTo = values[2];
            String[] ipFromParts = ipFrom.split("\\.");
            String[] ipToParts = ipTo.split("\\.");
            
            if (ipFromParts.length > 1 && ipToParts.length > 1) {
            	if (Integer.parseInt(ipFromParts[0]) < 10)
            		ipFromParts[0] = "00" + ipFromParts[0];
            	else if (Integer.parseInt(ipFromParts[0]) < 100)
            		ipFromParts[0] = "0" + ipFromParts[0];
            	
            	if (Integer.parseInt(ipFromParts[1]) < 10)
            		ipFromParts[1] = "00" + ipFromParts[1];
            	else if (Integer.parseInt(ipFromParts[1]) < 100)
            		ipFromParts[1] = "0" + ipFromParts[1];
            	
            	if (Integer.parseInt(ipFromParts[2]) < 10)
            		ipFromParts[2] = "00" + ipFromParts[2];
            	else if (Integer.parseInt(ipFromParts[2]) < 100)
            		ipFromParts[2] = "0" + ipFromParts[2];
            	
            	if (Integer.parseInt(ipFromParts[3]) < 10)
            		ipFromParts[3] = "00" + ipFromParts[3];
            	else if (Integer.parseInt(ipFromParts[3]) < 100)
            		ipFromParts[3] = "0" + ipFromParts[3];
            	
            	ipFrom = ipFromParts[0] + "." + ipFromParts[1] + "." + ipFromParts[2] + "." + ipFromParts[3];
            	
            	if (Integer.parseInt(ipToParts[0]) < 10)
            		ipToParts[0] = "00" + ipToParts[0];
            	else if (Integer.parseInt(ipToParts[0]) < 100)
            		ipToParts[0] = "0" + ipToParts[0];
            	
            	if (Integer.parseInt(ipToParts[1]) < 10)
            		ipToParts[1] = "00" + ipToParts[1];
            	else if (Integer.parseInt(ipToParts[1]) < 100)
            		ipToParts[1] = "0" + ipToParts[1];
            	
            	if (Integer.parseInt(ipToParts[2]) < 10)
            		ipToParts[2] = "00" + ipToParts[2];
            	else if (Integer.parseInt(ipToParts[2]) < 100)
            		ipToParts[2] = "0" + ipToParts[2];
            	
            	if (Integer.parseInt(ipToParts[3]) < 10)
            		ipToParts[3] = "00" + ipToParts[3];
            	else if (Integer.parseInt(ipToParts[3]) < 100)
            		ipToParts[3] = "0" + ipToParts[3];
            	
            	ipTo = ipToParts[0] + "." + ipToParts[1] + "." + ipToParts[2] + "." + ipToParts[3];
            	
            	String min = ipFrom, max = ipTo;
            	if (ipFrom.compareTo(ipTo) > 0) {
            		min = ipTo;
            		max = ipFrom;
            	}		
            	
            	String keyFrom = IPMap.ceilingKey(min);
            	String keyTo = IPMap.ceilingKey(max);
            	int partitionNum;
            	if (keyFrom == null) {
            		if (keyTo == null)
            			partitionNum = partition_no * partition_no - 1;
            		else
            			partitionNum = (partition_no - 1) * partition_no + IPMap.get(keyTo);
            	}
            	else {
            		if (keyTo == null)
            			partitionNum = IPMap.get(keyFrom) * partition_no + partition_no - 1;
            		else
            			partitionNum = IPMap.get(keyFrom) * partition_no + IPMap.get(keyTo);
            	}
            	context.write(new IntWritable(partitionNum), new Text(ipFrom));
            	context.write(new IntWritable(partitionNum), new Text(ipTo));
            }
        }
     } 
 
 	public static class CustomPartitioner extends Partitioner<IntWritable, Text> { 
 		@Override
 		public int getPartition(IntWritable key, Text value, int numReduceTasks) {
 			return key.get();
 		}
 	} 
 
     public static class Reduce extends Reducer<IntWritable, Text, Text, NullWritable> { 
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
          throws IOException, InterruptedException {
        	HashSet<String> set = new HashSet<String>();
        	
        	for (Text val : values) {
        		set.add(val.toString());
        	}
        	
        	ArrayList<String> unique = new ArrayList<String>(set);
        	Collections.sort(unique);
        	
            for (String u : unique) {
            	context.write(new Text(u), NullWritable.get());
            }
        }
     }        
     
     public static void main(String[] args) throws Exception {
    	final String NAME_NODE = "hdfs://hadoop-master:9000";
    	final int partition_no = 148;
        Configuration conf = new Configuration();  
        Job job = new Job(conf, "uniqueips");

        job.setJarByClass(PrepareSflows.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setPartitionerClass(CustomPartitioner.class);
        DistributedCache.addCacheFile(new URI(NAME_NODE + "/user/root/" + args[2]), job.getConfiguration());
        
        TextOutputFormat.setCompressOutput(job, true);
	    TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        
        job.setNumReduceTasks(partition_no * partition_no);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));        
        job.waitForCompletion(true);
     }        
   }
