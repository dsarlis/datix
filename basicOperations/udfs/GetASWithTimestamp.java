package diplom;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.TreeMap;

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

public class GetASWithTimestamp extends UDF {
	private TreeMap<Long, HashMap<String, String>> ASMap;
	private KdTree<Long> kd;
	private final String TABLE_NAME = "ases";

	public String evaluate (int partNum, String tree_partition, String dateIn, String ip) throws HiveException, IOException, ParseException {
        if (ASMap == null) {
            ASMap = new TreeMap <Long, HashMap<String, String>>();
            
            Configuration conf = HBaseConfiguration.create();
    	    conf.set("hbase.table.name", TABLE_NAME);
            HTable table = new HTable(conf, TABLE_NAME);
            
            Scan scan = new Scan();
            scan.setCaching(20000);
            scan.setCacheBlocks(false);
            scan.setBatch(20000);
            scan.addFamily(Bytes.toBytes("asnames"));
            scan.setMaxVersions(12);
            
            FileSystem fs = FileSystem.get(new Configuration());
    		Path pt = new Path("hdfs://hadoop-master:9000/user/root/" + tree_partition);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            kd = new KdTree<Long>(br);
            br.close();
            
            double bounds[][] = kd.findBounds(partNum);
            
            double startDateDouble = bounds[0][2];
            double stopDateDouble = bounds[1][2];
            
            //fix date ranges: from the first day of the first month
            //till the last day of the second month
    		Date  startDate = new Date((long) startDateDouble);
    		Date stopDate = new Date((long) stopDateDouble);
            String startDateStr = new SimpleDateFormat("yyyy-MM-dd").format(startDate);
            String[] parts = startDateStr.split("-");
            startDateStr = parts[0]+"-" + parts[1] + "-" + "01";
            String stopDateStr = new SimpleDateFormat("yyyy-MM-dd").format(stopDate);
            parts = stopDateStr.split("-");
            stopDateStr = parts[0]+"-" + parts[1] + "-" + "31";
            scan.setTimeRange((long) new SimpleDateFormat("yyyy-MM-dd").parse(startDateStr).getTime(), 
            									(long) new SimpleDateFormat("yyyy-MM-dd").parse(stopDateStr).getTime());
            
            //get key-value pairs from HBase
            ClientScanner scanner = (ClientScanner) table.getScanner(scan);
            Result result;
            while ((result = scanner.next()) != null) {            	            	
            	NavigableMap<byte[],NavigableMap<Long,byte[]>> map = result.getMap().get(Bytes.toBytes("asnames"));
            	long key = Long.parseLong(Bytes.toString(result.getRow()));
            	//for each column qualifier, i.e. "start" or "stop"
            	if (map.containsKey(Bytes.toBytes("start"))) {
            		NavigableMap<Long, byte[]> timeMap = map.get(Bytes.toBytes("start"));
            		//take each timestamp version and put it in ASMap
            		for (Long t : timeMap.keySet()) {
            			Date date = new Date(t);
            			String month = new SimpleDateFormat("yyyy-MM-dd").format(date).split("-")[1];
            			HashMap<String, String> hashmap;
            			if (!ASMap.containsKey(key)) {
            				hashmap = new HashMap<String, String>();
            				hashmap.put(month, Bytes.toString(timeMap.get(t)) + "_start");
            			}
            			else {
            				hashmap = ASMap.get(key);
            				hashmap.put(month, Bytes.toString(timeMap.get(t)) + "_start");
            			}
            			ASMap.put(Long.parseLong(Bytes.toString(result.getRow())), hashmap);
            		}
            	}
            	else {
            		NavigableMap<Long, byte[]> timeMap = map.get(Bytes.toBytes("stop"));
            		//take each timestamp version and put it in ASMap
            		for (Long t : timeMap.keySet()) {
            			Date date = new Date(t);
        				String month = new SimpleDateFormat("yyyy-MM-dd").format(date).split("-")[1];
        				HashMap<String, String> hashmap;
            			if (!ASMap.containsKey(key)) {
            				hashmap = new HashMap<String, String>();
            				hashmap.put(month, Bytes.toString(timeMap.get(t)) + "_stop");
            			}
            			else {
            				hashmap = ASMap.get(key);
            				hashmap.put(month, Bytes.toString(timeMap.get(t)) + "_stop");
            			}
            			ASMap.put(Long.parseLong(Bytes.toString(result.getRow())), hashmap);
            		}
            	}
            }          
            table.close();
        }

        String[] ipParts = ip.split("\\.");
        if (ipParts.length > 1) {
        	long intIP = Long.parseLong(ipParts[0]) * 16777216 + Long.parseLong(ipParts[1]) * 65536 
        				 + Long.parseLong(ipParts[2]) * 256 + Long.parseLong(ipParts[3]);
        	Long key = ASMap.ceilingKey(intIP);
        	//find month of each record and search ASMap for the appropriate value
        	String month = dateIn.split("-")[1];
        	if (key != null) {
        		if (key == intIP)
        			return ASMap.get(key).get(month).split("_")[0];
        		else {
        			String[] part = ASMap.get(key).get(month).split("_");
        			if (part[1].equals("stop"))
        				return part[0];
        		}
        	}
		}

        return null;
    }
}
