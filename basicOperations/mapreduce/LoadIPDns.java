package diplom;

import java.io.IOException;
//import java.net.URI;
//import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
//import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class LoadIPDns {
	private static final String TABLE_NAME = "rdns"; 
	public static class Map extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	    ImmutableBytesWritable hkey = new ImmutableBytesWritable();
	    static int count = 0;
	        
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] fields = line.split(",");
	        count++;
	        
	        if (fields.length == 2) { 
	        	//System.out.println("Line read: " + line);
	        	String[] ipParts = fields[0].split("\\.");
	        	String ip = fields[0];
	        	
	        	if (Integer.parseInt(ipParts[0]) < 10)
            		ipParts[0] = "00" + ipParts[0];
            	else if (Integer.parseInt(ipParts[0]) < 100)
            		ipParts[0] = "0" + ipParts[0];
	        	
	        	if (Integer.parseInt(ipParts[1]) < 10)
            		ipParts[1] = "00" + ipParts[1];
            	else if (Integer.parseInt(ipParts[1]) < 100)
            		ipParts[1] = "0" + ipParts[1];
	        	
	        	if (Integer.parseInt(ipParts[2]) < 10)
            		ipParts[2] = "00" + ipParts[2];
            	else if (Integer.parseInt(ipParts[2]) < 100)
            		ipParts[2] = "0" + ipParts[2];
	        	
	        	if (Integer.parseInt(ipParts[3]) < 10)
            		ipParts[3] = "00" + ipParts[3];
            	else if (Integer.parseInt(ipParts[3]) < 100)
            		ipParts[3] = "0" + ipParts[3];
	        	
	        	ip = ipParts[0] + "." + ipParts[1] + "." + ipParts[2] + "." + ipParts[3];
            	
	        	hkey.set(ip.getBytes());
	        	KeyValue kv = new KeyValue(hkey.get(), Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"), 
	        						   Bytes.toBytes(fields[1]));
	        
	        	context.write(hkey, kv);
	        	value.clear();
	        	hkey = new ImmutableBytesWritable();
	        }
	    }
	    public void cleanup (Context context) {
	    		System.out.println(count);
	    }
	 }

	@SuppressWarnings("unused")
	private static void loadHFiles()throws Exception {
	   	Configuration conf = HBaseConfiguration.create();
 	    conf.addResource("hbase-default.xml");
 	    conf.addResource("hbase-site.xml");
 	    HBaseAdmin hadmin = new HBaseAdmin(conf);
 		Path hfofDir= new Path("rdns_result/dnsnames");
 		FileSystem fs = hfofDir.getFileSystem(conf);
 	    //if (!fs.exists(hfofDir)) {
 	    //  throw new FileNotFoundException("HFileOutputFormat dir " +
 	    //      hfofDir + " not found");
 	    //}
 	   // FileStatus[] familyDirStatuses = fs.listStatus(hfofDir);
 	    //if (familyDirStatuses == null) {
 	    //  throw new FileNotFoundException("No families found in " + hfofDir);
 	    //}
 	    int length =0;
 	    byte[][] splits = new byte[18000][];
		Path[] hfiles = FileUtil.stat2Paths(fs.listStatus(hfofDir));
		for (Path hfile : hfiles) {
			if (hfile.getName().startsWith("_")) continue;
		
		  		HFile.Reader hfr = HFile.createReader(fs, hfile, new CacheConfig(conf));
		  		//HFile.Reader hfr = 	new HFile.Reader(fs, hfile, null, false);
		  		final byte[] first;
		  		try {
		  			hfr.loadFileInfo();
		  			first = hfr.getFirstRowKey();
		  		}  finally {
		  			hfr.close();
		  		}
				//System.out.println("out/I/"+hfile.getName()+" \t "+Bytes.toStringBinary(first));
		  		splits[length]=first.clone();
		  		length++;
		}
		byte[][] splits1 = new byte[length][];

 	    for (int i = 0; i < splits1.length; i++) {
 	    		splits1[i]=splits[i];
 		}
 	    Arrays.sort(splits1, Bytes.BYTES_COMPARATOR);
 	  
 		
 	    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
 	    
 		HColumnDescriptor family= new HColumnDescriptor("dnsnames");
 		//family.setCompressionType(Algorithm.SNAPPY);
 		desc.addFamily(family);
 		//family= new HColumnDescriptor("C");
 		//desc.addFamily(family); 
 		//for (int i = 0; i < splits.length; i++) {
 		//	System.out.println(Bytes.toStringBinary(splits[i]));
 		//}
 		conf.setInt("zookeeper.session.timeout", 600000);
 		if(hadmin.tableExists(TABLE_NAME)){
 			//hadmin.disableTable(TABLE_NAME);
 			//hadmin.deleteTable(TABLE_NAME);
 		}
 		else{
 			hadmin.createTable(desc, splits1);
 		}
 		//hadmin.createTable(desc);
 		String[] args1 = new String[2];
 		args1[0]="rdns_result";
 		args1[1]=TABLE_NAME;
 		//args1[1]="new2";
 		
 		ToolRunner.run(new LoadIncrementalHFiles(HBaseConfiguration.create()), args1);
 		hadmin.close();
 	
	}
	        
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("hbase.table.name", TABLE_NAME);    
	    
	    Job job = new Job(conf, "loadipdns");
	    
	    job.setJarByClass(LoadIPDns.class);
	    job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(KeyValue.class);
	    
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reducer.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(HFileOutputFormat.class);
	    
	    HTable hTable = new HTable(conf, TABLE_NAME);
	    HFileOutputFormat.configureIncrementalLoad(job, hTable);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    //job.setPartitionerClass(TotalOrderPartitioner.class);
        //job.setNumReduceTasks(113);
        
        
        
        //Path input = new Path(args[2]);
        //input = input.makeQualified(input.getFileSystem(conf));
        //Path partitionFile = new Path(input, "part-r-00000");
        //System.out.println("Partition file path:" + partitionFile);
        //TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);
        
        //URI partitionUri = new URI(partitionFile.toString() + "#_partitions");
        //DistributedCache.addCacheFile(partitionUri, job.getConfiguration());
        //DistributedCache.createSymlink(job.getConfiguration());
	    
	    job.waitForCompletion(true);
	    
	    //LoadIPDns.loadHFiles();
	    
	 }
}
