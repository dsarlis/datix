package diplom;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ClientScanner;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import ags.utils.dataStructures.trees.thirdGenKD.KdTree;

public class GetDnsWithTimestamp  extends UDF {
	private KdTree<Long> kd;
	private HashMap<String, HashMap<String, String>> DnsMap;
	private final String TABLE_NAME = "rdns_2";
	
	private void fillMap(int partNum, String tree_partition, String uniqueIP, int dimensions) 
			throws IOException, ParseException {
		DnsMap = new HashMap <String, HashMap<String, String>>();
        
        FileSystem fs = FileSystem.get(new Configuration());
		Path pt = new Path("hdfs://hadoop-master:9000/user/root/" + tree_partition);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        kd = new KdTree<Long>(br);
        br.close();
        
        
        double[] min = new double[dimensions];
        double[] max = new double[dimensions];
        for (int i = 0; i < dimensions; i ++) {
        	min[i] = 0;
        	max[i] = (double) Long.MAX_VALUE;
        }
			
        List<Integer> l = new ArrayList<Integer>();
        kd.findBuckets(min, max, l);
			Collections.sort(l);
			partNum = l.indexOf(partNum);
			if (partNum < 0) {
				System.err.println("Internal error. No such partition");
				System.exit(-1);
			}
		String partNo = "" + partNum;
        
        if (partNum < 10000)
        	partNo = "0" + partNum;
        if (partNum < 1000)
        	partNo = "00" + partNum;
        if (partNum < 100)
        	partNo = "000" + partNum;
        if (partNum < 10)
        	partNo = "0000" + partNum;
        
        double bounds[][] = kd.findBounds(partNum);
        
        double startDateDouble = bounds[0][2];
        double stopDateDouble = bounds[1][2];
        
        //fix date ranges: from the first day of the first month
        //till the last day of the second month
		Date  startDate = new Date((long) startDateDouble);
		Date stopDate = new Date((long) stopDateDouble);
        String startDateStr = new SimpleDateFormat("yyyy-MM-dd").format(startDate);
        String[] parts = startDateStr.split("-");
     // if it's on the first days of the dataset fix month
        if (parts[0].equals("1970"))
        	parts[1] = "07";
        startDateStr = parts[0]+"-" + parts[1] + "-" + "01";
        String stopDateStr = new SimpleDateFormat("yyyy-MM-dd").format(stopDate);
        parts = stopDateStr.split("-");
     // if it's on the last days of the dataset fix month 
        if (parts[0].length() > 4)
        	parts[1] = "02";
        stopDateStr = parts[0]+"-" + parts[1] + "-" + "31";
        
        
        Configuration conf = HBaseConfiguration.create();
	    conf.set("hbase.table.name", TABLE_NAME);
        
        HTable table = new HTable(conf, TABLE_NAME);
        
        Scan scan = new Scan();
        scan.setCaching(20000);
        scan.setCacheBlocks(false);
        scan.setBatch(20000);
        scan.addFamily(Bytes.toBytes("dnsnames"));
        scan.setMaxVersions(12);
        scan.setTimeRange((long) new SimpleDateFormat("yyyy-MM-dd").parse(startDateStr).getTime(), 
				(long) new SimpleDateFormat("yyyy-MM-dd").parse(stopDateStr).getTime());
       
        
        pt = new Path("hdfs://hadoop-master:9000/user/root/" + uniqueIP + "/part-r-" + partNo);
        //fs = FileSystem.get(new Configuration());
        FileScanner buffered = new FileScanner(new BufferedReader(new InputStreamReader(fs.open(pt))));
        String line = buffered.next();
        
        scan.setStartRow(Bytes.toBytes(line));
        ClientScanner scanner = (ClientScanner) table.getScanner(scan);
        Result result = scanner.next();
        
        long start = System.currentTimeMillis();
        while (line != null && result != null) {
        	
        	if (Bytes.toString(result.getRow()).equals(line)) {
        		NavigableMap<byte[],NavigableMap<Long,byte[]>> map = result.getMap().get(Bytes.toBytes("dnsnames"));
            	String key = Bytes.toString(result.getRow());
            	NavigableMap<Long, byte[]> timeMap = map.get(Bytes.toBytes("rdns"));
            	//take each timestamp version and put it in DnsMap
            	for (Long t : timeMap.keySet()) {
            		Date date = new Date(t);
            		String month = new SimpleDateFormat("yyyy-MM-dd").format(date).split("-")[1];
            		HashMap<String, String> hashmap;
            		if (!DnsMap.containsKey(key)) {
            			hashmap = new HashMap<String, String>();
            			hashmap.put(month, Bytes.toString(timeMap.get(t)));
            		}
            		else {
            			hashmap = DnsMap.get(key);
            			hashmap.put(month, Bytes.toString(timeMap.get(t)));
            		}
            		DnsMap.put(Bytes.toString(result.getRow()), hashmap);
            	}
            	result = scanner.next();
            	line = buffered.next();
            }
        	else {
        		String[] uniqueParts = line.split("\\.");
        		String[] scanIPparts = Bytes.toString(result.getRow()).split("\\.");
    			
        		long intUniqueIP = Long.parseLong(uniqueParts[0]) * 16777216 + Long.parseLong(uniqueParts[1]) * 65536 
        				 + Long.parseLong(uniqueParts[2]) * 256 + Long.parseLong(uniqueParts[3]);
    		
        		long intScanIP = Long.parseLong(scanIPparts[0]) * 16777216 + Long.parseLong(scanIPparts[1]) * 65536 
   				 + Long.parseLong(scanIPparts[2]) * 256 + Long.parseLong(scanIPparts[3]);
    		
        		//find which pointer should be forwarded a few positions
        		if (intUniqueIP < intScanIP) {
        			line = buffered.seekTo(Bytes.toString(result.getRow()));
        		}
        		else {
        			result = scanner.seekTo(Bytes.toBytes(line), intUniqueIP - intScanIP);
        		}
        	}
        	
    		if (line == null || result == null)
    			break;
    		
        }
        buffered.close();	            
        long stop = System.currentTimeMillis();
        System.out.println("Time to fill DnsMap: " + ((stop-start)/1000) + "secs");
        System.out.println("Size of hashmap: " + DnsMap.size());
        table.close();
	}
	
	public String evaluate(int partNum, String tree_partition, String uniqueIP, int dimensions, 
			String dateIn, String ip) throws HiveException, IOException, ParseException {
        if (DnsMap == null) {
            fillMap(partNum, tree_partition, uniqueIP, dimensions);
        }

        String[] ipParts = ip.split("\\.");
        
        
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
        String month = dateIn.split("-")[1];
        //System.out.println("input ip: " + ip);
        return DnsMap.get(ip).get(month);
    }
	
	public String evaluate(int partNum, String tree_partition, String uniqueIP, int dimensions, 
			String dateIn, String ipFrom, String ipTo) throws HiveException, IOException, ParseException {
        if (DnsMap == null) {
            fillMap(partNum, tree_partition, uniqueIP, dimensions);
        }

        String[] ipFromParts = ipFrom.split("\\.");
        
        
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
        //System.out.println("input ip: " + ip);
        
        String[] ipToParts = ipTo.split("\\.");
        
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
    	String month = dateIn.split("-")[1];
        
        return DnsMap.get(ipFrom).get(month) + " " + DnsMap.get(ipTo).get(month);
    }
}
