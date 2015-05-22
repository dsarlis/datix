package diplom;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import ags.utils.dataStructures.trees.thirdGenKD.KdTree;

public class GetASWithTimestamp extends UDF {
	private TreeMap<Long, HashMap<String, String>> ASMap;
	private KdTree<Long> kd;

	private void fillMap(int partNum, String tree_partition) throws IOException, ParseException, HiveException {
		ASMap = new TreeMap <Long, HashMap<String, String>>();
        
		/*
        Configuration conf = HBaseConfiguration.create();
	    conf.set("hbase.table.name", TABLE_NAME);
        HTable table = new HTable(conf, TABLE_NAME);
        
        Scan scan = new Scan();
        scan.setCaching(20000);
        scan.setCacheBlocks(false);
        scan.setBatch(20000);
        scan.addFamily(Bytes.toBytes("asnames"));
        scan.setMaxVersions(2);
        */
        
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
        String startMonth = parts[1];
        String stopDateStr = new SimpleDateFormat("yyyy-MM-dd").format(stopDate);
        parts = stopDateStr.split("-");
        String stopMonth = parts[1];
        // if it's on the last days of the dataset put the limit explicitly 
        // because the value is too large to be formatted correctly
        if (parts[0].length() > 4)
        	stopMonth = "02";
        /*
        scan.setTimeRange((long) new SimpleDateFormat("yyyy-MM-dd").parse(startDateStr).getTime(), 
        									(long) new SimpleDateFormat("yyyy-MM-dd").parse(stopDateStr).getTime());
        
        System.out.println("Before fetching data from HBase");
        long start = System.currentTimeMillis();
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
        long stop = System.currentTimeMillis();
        System.out.println("Time to fill ASMap: " + ((stop-start)/1000) + "secs");
        System.out.println("Size of hashmap: " + ASMap.size());
        */
		
		try {
            BufferedReader lineReader = new BufferedReader(new FileReader("GeoIPASNum2_" + startMonth + ".csv"));

            String line = null;
            while ((line = lineReader.readLine()) != null) {
                String[] fields = line.split(",");
                long ipStart = Long.parseLong(fields[0]);
                long ipEnd = Long.parseLong(fields[1]);
                String as = fields[2].replace("\"", "");
                HashMap<String, String> hashmap = new HashMap<String, String>();
                hashmap.put(startMonth, as + "_start");
                ASMap.put(ipStart, hashmap);
                hashmap = new HashMap<String, String>();
                hashmap.put(startMonth, as + "_stop");
                ASMap.put(ipEnd, hashmap);
            }
            lineReader.close();
        } catch (FileNotFoundException e) {
            throw new HiveException("GeoIPASNum2_" + startMonth + ".csv" + " doesn't exist");
        } catch (IOException e) {
            throw new HiveException("process file " + "GeoIPASNum2_" + startMonth + ".csv" + " failed, please check format");
        }
		
		if (!startMonth.equals(stopMonth)) {
			try {
				BufferedReader lineReader = new BufferedReader(new FileReader("GeoIPASNum2_" + stopMonth + ".csv"));

				String line = null;
				while ((line = lineReader.readLine()) != null) {
					String[] fields = line.split(",");
					long ipStart = Long.parseLong(fields[0]);
					long ipEnd = Long.parseLong(fields[1]);
					String as = fields[2].replace("\"", "");
					HashMap<String, String> hashmap;
					if (!ASMap.containsKey(ipStart)) {
						hashmap = new HashMap<String, String>();
					}
					else {
						hashmap = ASMap.get(ipStart);	
					}
					hashmap.put(stopMonth, as + "_start");
					ASMap.put(ipStart, hashmap);
					if (!ASMap.containsKey(ipEnd)) {
						hashmap = new HashMap<String, String>();
					}
					else {
						hashmap = ASMap.get(ipEnd);
					}
					hashmap.put(stopMonth, as + "_stop");
					ASMap.put(ipEnd, hashmap);
				}
				lineReader.close();
			} catch (FileNotFoundException e) {
				throw new HiveException("GeoIPASNum2_" + startMonth + ".csv" + " doesn't exist");
			} catch (IOException e) {
				throw new HiveException("process file " + "GeoIPASNum2_" + startMonth + ".csv" + " failed, please check format");
			}
		}
		
		/*HashMap<String, String> hashmap = ASMap.get((long) 16777216);
		for (String key : hashmap.keySet()) {
			System.out.println("month: " + key + " value: " + hashmap.get(key));
		} 
		hashmap = ASMap.get((long) 16777471);
		for (String key : hashmap.keySet()) {
			System.out.println("month: " + key + " value: " + hashmap.get(key));
		} */
	}
	
	public String evaluate (int partNum, String tree_partition, String dateIn, String ip) 
			throws HiveException, IOException, ParseException {
        if (ASMap == null) {
            fillMap(partNum, tree_partition);
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
	
	public String evaluate (int partNum, String tree_partition, String dateIn, String ipFrom, String ipTo) 
			throws HiveException, IOException, ParseException {
        if (ASMap == null) {
            fillMap(partNum, tree_partition);
        }

        String[] ipFromParts = ipFrom.split("\\.");
        String[] ipToParts = ipTo.split("\\.");
        if (ipFromParts.length > 1 && ipToParts.length > 1) {
        	long intIPFrom = Long.parseLong(ipFromParts[0]) * 16777216 + Long.parseLong(ipFromParts[1]) * 65536 
        				 + Long.parseLong(ipFromParts[2]) * 256 + Long.parseLong(ipFromParts[3]);
        	long intIPTo = Long.parseLong(ipToParts[0]) * 16777216 + Long.parseLong(ipToParts[1]) * 65536 
   				 + Long.parseLong(ipToParts[2]) * 256 + Long.parseLong(ipToParts[3]);
        	Long keyFrom = ASMap.ceilingKey(intIPFrom);
        	Long keyTo = ASMap.ceilingKey(intIPTo);
        	//find month of each record and search ASMap for the appropriate value
        	String month = dateIn.split("-")[1];
        	if (keyFrom == null && keyTo == null) {
        		return null + " " + null;
        	}
        	if (keyTo == null) {
        		if (keyFrom == intIPFrom)
        			return ASMap.get(keyFrom).get(month).split("_")[0] + " " + null; 
        		else {
        			String[] part = ASMap.get(keyFrom).get(month).split("_");
        			if (part[1].equals("stop"))
        				return part[0] + " " + null;
        		}
        	}
        	if (keyFrom == null) {
        		if (keyTo == intIPTo)
        			return  null + " " + ASMap.get(keyTo).get(month).split("_")[0]; 
        		else {
        			String[] part = ASMap.get(keyTo).get(month).split("_");
        			if (part[1].equals("stop"))
        				return null + " " + part[0];
        		}
        	}
        	
        	if (keyFrom == intIPFrom) {
        		if (keyTo == intIPTo)
        			return ASMap.get(keyFrom).get(month).split("_")[0] + " " + ASMap.get(keyTo).get(month).split("_")[0];
        		else {
        			String[] partTo = ASMap.get(keyTo).get(month).split("_");
        			if (partTo[1].equals("stop"))
        				return ASMap.get(keyFrom).get(month).split("_")[0] + " " + partTo[0];
        		}
        	}
        	else {
        		String[] part = ASMap.get(keyFrom).get(month).split("_");
        		if (part[1].equals("stop")) {
        			if (keyTo == intIPTo)
        				return part[0] + " " + ASMap.get(keyTo).get(month).split("_")[0];
        			else {
        				String[] partTo = ASMap.get(keyTo).get(month).split("_");
        				if (partTo[1].equals("stop"))
        					return part[0] + " " + partTo[0];
        			}
        		}
        	}
        }
        return null + " " + null;
    }
}
