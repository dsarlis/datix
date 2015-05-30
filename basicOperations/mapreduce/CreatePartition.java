package diplom;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import ags.utils.dataStructures.trees.thirdGenKD.KdTree;


public class CreatePartition {
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      	private Text word = new Text();
      	private float sampleRate;
		
      	@Override
      	public void setup(Context context) {
      		Configuration conf = context.getConfiguration();
      		sampleRate = Float.parseFloat(conf.get("sampleRate"));
      	}
      	
		@Override
		public void run(Context context) throws IOException, InterruptedException {
			setup(context);
			while (context.nextKeyValue()) {
				Random ran = new Random();
				if (ran.nextDouble() < sampleRate)
					map(context.getCurrentKey(), context.getCurrentValue(), context);
			}
			cleanup(context);
		}
			
		
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            word.set(line);
            context.write(new Text("sflows"), word);
        }
    }
           
 
 
     public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {
    	 private int bucketSize;
    	 private String[] dimensions;
    	 private String tree_partition;
    	 
    	 public void setup(Context context) {
    		Configuration conf = context.getConfiguration();
       		dimensions = conf.get("dimensions").split(",");
       		bucketSize = Integer.parseInt(conf.get("bucketSize"));
       		tree_partition = conf.get("tree_partition");
    	 }
    	 
    	 public void reduce(Text key, Iterable<Text> values, Context context)
          throws IOException, InterruptedException {
    		 KdTree<Long> kd = new KdTree<Long>(dimensions.length, bucketSize);
    		 
    		 for (Text val : values) {
    			 String[] parts = val.toString().split(" ");
    			 String[] ipFrom = parts[0].split("\\.");
    			 String[] ipTo = parts[2].split("\\.");
    			 
    			 if (ipFrom.length > 1 && ipTo.length > 1) {
    				 double[] point = new double[dimensions.length];
    				 for (int i = 0; i < dimensions.length; i++) {
    					 switch (Integer.parseInt(dimensions[i])) {
    			            case 1:  point[i] = Double.parseDouble(parts[1]);
    			                     break;
    			            case 3:  point[i] = Double.parseDouble(parts[3]);
    			                     break;
    			            case 8:  SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
									 try {
									 	Date dateStr = formatter.parse(parts[8]);
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
    			 
    				 kd.addPoint(point, (long) 100);
    			 }
    		 }
    		 
    		 System.out.println(kd.countLeafs());
    		 FileSystem fs = FileSystem.get(context.getConfiguration());
    		 Path pt=new Path("hdfs://hadoop-master:9000/user/root/" + tree_partition);
             BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
             kd.printTree(writer);
             writer.close();
    	 }
    }
     
     public static void main(String[] args) throws Exception {
    	if (args.length < 5) {
    		System.out.println("Wrong number of arguments. Usage <input_path> <output_dir> <tree_output> <sampleRate> <dimensions> <bucketsize>");
    		System.out.println("Example: CreatePartition sflows result_tree tree_partition 0.01 0,2,8 100000");
    		System.exit(0);
    	}
        Configuration conf = new Configuration(); 
        conf.set("tree_partition", args[2]);
        conf.set("sampleRate", args[3]);
        conf.set("dimensions", args[4]);
        conf.set("bucketSize", args[5]);
        Job job = new Job(conf, "partition_tree");
        
        job.setJarByClass(CreatePartition.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);     
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);  
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));        
        job.waitForCompletion(true);
     }        
}
