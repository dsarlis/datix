package diplom;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import ags.utils.dataStructures.trees.thirdGenKD.KdTree;



public class PartitionSflows {
 	private static KdTree<Long> kd = null;
 	private static List<Integer> l = new ArrayList<Integer>();
 	
 	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
	 	private String[] dimensions = null;
	 
	 	public void setup(Context context) throws IOException {
	 		dimensions = context.getConfiguration().get("dimensions").split(",");
			Path[] uris = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			BufferedReader br = new BufferedReader(new FileReader(uris[0].toString()));
			kd = new KdTree<Long>(br);
			double[] min = new double[dimensions.length];
 			for (int i = 0; i < dimensions.length; i++){
 				min[i] = 0;
 			}
 			double[] max = new double[dimensions.length];
 			for (int i = 0; i < dimensions.length; i++) {
 				max[i] = (double) Long.MAX_VALUE;
 			}
 			
 			kd.findBuckets(min, max, l);
 			Collections.sort(l);
 			//System.out.println(l.size());
			br.close();
		}
	 
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split(" ");
           		
            double[] point = new double[dimensions.length];
            
            for (int i = 0; i < dimensions.length; i++) {
            	switch (Integer.parseInt(dimensions[i])) {
	            	case 1:  point[i] = Double.parseDouble(values[1]);
	            			 break;
	            	case 3:  point[i] = Double.parseDouble(values[3]);
	                     	 break;
	            	case 8:  SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
						 	 try {
						 		 Date dateStr = formatter.parse(values[8]);
						 		 point[i] = (double) dateStr.getTime();
						 	 } catch (ParseException e) {
						 		 // TODO Auto-generated catch block
						 		 e.printStackTrace();
						 	 }
						 	 break;
	                     
	            	default: System.err.println("Dimension number n");
	            		 	 break;
            	}
            }
            
            int partitionNum = kd.find(point);
            context.write(new IntWritable(partitionNum), new Text(line + " " + partitionNum));
        }
     } 
 
 	public static class CustomPartitioner extends Partitioner<IntWritable, Text> {
 		@Override
 		public int getPartition(IntWritable key, Text value, int numReduceTasks) {
 			return l.indexOf(key.get());
 		}
 	} 
 
     public static class Reduce extends Reducer<IntWritable, Text, Text, NullWritable> { 
    	 private HashSet<String> set = new HashSet<String>();
    	 
    	 @Override
    	 public void cleanup(Context context) throws IOException {
    		String uniqueIP = context.getConfiguration().get("uniqueIP");
    		ArrayList<String> unique = new ArrayList<String>(set);
         	Collections.sort(unique);
         	
         	JobConf job = (JobConf) context.getConfiguration();
         	int partition_id = Integer.parseInt(job.get("mapred.task.id").split("_")[4]);
         	String partNo = "" + partition_id;
            
            if (partition_id < 10000)
            	partNo = "0" + partition_id;
            if (partition_id < 1000)
            	partNo = "00" + partition_id;
            if (partition_id < 100)
            	partNo = "000" + partition_id;
            if (partition_id < 10)
            	partNo = "0000" + partition_id;
            
         	FileSystem fs = FileSystem.get(context.getConfiguration());
         	Path pt=new Path("hdfs://hadoop-master:9000/user/root/" + uniqueIP + "/part-r-" + partNo);
            BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
            for (String a : unique) {
            	writer.write(a);
            	writer.newLine();
            }
            writer.close();
    	 }
    	 
    	 
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
          throws IOException, InterruptedException {
        	
            for (Text val : values) {
            	
            	String[] parts = val.toString().split(" ");
            	String ipFrom = parts[0];
                String ipTo = parts[2];
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
                	
                	set.add(ipFrom);
                	
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
                	set.add(ipTo);
                	context.write(val, NullWritable.get());
                }
            }
        }
     }        
     
     public static void main(String[] args) throws Exception {
    	final String NAME_NODE = "hdfs://hadoop-master:9000";
    	final int partition_no = Integer.parseInt(args[5]);
    	
        Configuration conf = new Configuration();  
        conf.set("dimensions", args[4]);
        conf.set("uniqueIP", args[3]);
        Job job = new Job(conf, "partitionsflows");

        job.setJarByClass(PartitionSflows.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setPartitionerClass(CustomPartitioner.class);
        job.getConfiguration().set("mapred.compress.map.output","true");
	    job.getConfiguration().set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.GzipCodec");
        DistributedCache.addCacheFile(new URI(NAME_NODE + "/user/root/" + args[2]), job.getConfiguration());
        
        TextOutputFormat.setCompressOutput(job, true);
	    TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        
        job.setNumReduceTasks(partition_no);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));        
        job.waitForCompletion(true);
     }        
   }

