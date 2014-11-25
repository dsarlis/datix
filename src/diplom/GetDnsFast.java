package diplom;

import java.io.BufferedReader;
//import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ClientScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class GetDnsFast extends UDF{
		private ArrayList<String> splitPointsList;
		private HashMap<String, String> DnsMap;
		private int partition_no = 148;
		
		public String evaluate(int partNum, String splitPointsFile, String ip) throws HiveException, IOException {
	        if (DnsMap == null) {
	            DnsMap = new HashMap <String, String>();
	            splitPointsList = new ArrayList <String>();
	            try {
	                BufferedReader lineReader = new BufferedReader(new FileReader(splitPointsFile));

	                String line = null;
	                while ((line = lineReader.readLine()) != null) {
	                    splitPointsList.add(line);
	                }
	                lineReader.close();
	            } catch (FileNotFoundException e) {
	                throw new HiveException(splitPointsFile + " doesn't exist");
	            } catch (IOException e) {
	                throw new HiveException("process file " + splitPointsFile + " failed, please check format");
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
	            
	            
	            Configuration conf = HBaseConfiguration.create();
	    	    conf.set("hbase.table.name", "rdns_2");
	            
	            HTable table = new HTable(conf, "rdns_2");
	            
	            Scan scan = new Scan();
	            scan.setCaching(20000);
	            scan.setCacheBlocks(false);
	            scan.setBatch(20000);
	            scan.addColumn(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"));
	            
	            
	            if (partNum / partition_no != 0 && (partNum / partition_no) != (partition_no - 1)) {
	            	scan.setStartRow(Bytes.toBytes(splitPointsList.get(partNum / partition_no - 1)));
	            	//System.out.println("StartRow: " + splitPointsList.get(partNum / partition_no - 1));
	            	scan.setStopRow(Bytes.toBytes(splitPointsList.get(partNum / partition_no)));
	            	//System.out.println("StopRow: " + splitPointsList.get(partNum / partition_no));
	            }
	            
	            if (partNum / partition_no == 0) {
	            	scan.setStopRow(Bytes.toBytes(splitPointsList.get(partNum / partition_no)));
	            	//System.out.println("StopRow: " + splitPointsList.get(partNum / partition_no));
	            }
	            if (partNum / partition_no == partition_no - 1) { 
	            	scan.setStartRow(Bytes.toBytes(splitPointsList.get(partition_no - 1)));
	            	//System.out.println("StartRow:" + splitPointsList.get(partition_no - 1));
	            }
	            
	            //int count = 0;
	            Path pt=new Path("hdfs://hadoop-master:9000/user/root/uniqueIPs/part-r-" + partNo + ".gz");
                FileSystem fs = FileSystem.get(new Configuration());
	            InputStream fileStream = fs.open(pt);
	            InputStream gzipStream = new GZIPInputStream(fileStream);
	            Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
	            FileScanner buffered = new FileScanner(new BufferedReader(decoder));
	            String line = buffered.next();
	            
	            ClientScanner scanner = (ClientScanner) table.getScanner(scan);
	            Result result = scanner.next();
	            
	            while (line != null && result != null) {
	            	String[] uniqueParts = line.split("\\.");
	        		String[] scanIPparts = Bytes.toString(result.getRow()).split("\\.");
	        			
	        		long intUniqueIP = Long.parseLong(uniqueParts[0]) * 16777216 + Long.parseLong(uniqueParts[1]) * 65536 
		        				 + Long.parseLong(uniqueParts[2]) * 256 + Long.parseLong(uniqueParts[3]);
	        		
	        		long intScanIP = Long.parseLong(scanIPparts[0]) * 16777216 + Long.parseLong(scanIPparts[1]) * 65536 
	       				 + Long.parseLong(scanIPparts[2]) * 256 + Long.parseLong(scanIPparts[3]);
	        		
	        		if (intUniqueIP > intScanIP) {
	        			result = scanner.seekTo(Bytes.toBytes(line), intUniqueIP - intScanIP);
	        		}
	        		else {
	        			line = buffered.seekTo(Bytes.toString(result.getRow()));
	        		}
	        		
	        		if (line == null || result == null)
	        			break;
	        		
	        		if (Bytes.toString(result.getRow()).equals(line)) {
	        			//System.out.println("ip :" + Bytes.toString(result.getRow()));
		            	DnsMap.put(Bytes.toString(result.getRow()), Bytes.toString(result.getValue(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"))));
		            }
	        		
	            }
	            
	            
	            if (partNum % partition_no != 0 && (partNum % partition_no) != (partition_no - 1)) {
	            	scan.setStartRow(Bytes.toBytes(splitPointsList.get(partNum % partition_no - 1)));
	            	//System.out.println("StartRow: " + splitPointsList.get(partNum % partition_no - 1));
	            	scan.setStopRow(Bytes.toBytes(splitPointsList.get(partNum % partition_no)));
	            	//System.out.println("StopRow: " + splitPointsList.get(partNum % partition_no));
	            }
	            
	            if (partNum % partition_no == 0) {
	            	scan.setStopRow(Bytes.toBytes(splitPointsList.get(partNum % partition_no)));
	            	//System.out.println("StopRow: " + splitPointsList.get(partNum % partition_no));
	            }
	            if (partNum % partition_no == partition_no - 1) {
	            	scan.setStartRow(Bytes.toBytes(splitPointsList.get(partNum % partition_no - 1)));
	            	//System.out.println("StartRow: " + splitPointsList.get(partNum % partition_no - 1));
	            }
	            
	            scanner = (ClientScanner) table.getScanner(scan);
	            result = scanner.next();
	            //count = 0;
	            
	            while (line != null && result != null) {
	            	String[] uniqueParts = line.split("\\.");
	        		String[] scanIPparts = Bytes.toString(result.getRow()).split("\\.");
	        			
	        		long intUniqueIP = Long.parseLong(uniqueParts[0]) * 16777216 + Long.parseLong(uniqueParts[1]) * 65536 
		        				 + Long.parseLong(uniqueParts[2]) * 256 + Long.parseLong(uniqueParts[3]);
	        		
	        		long intScanIP = Long.parseLong(scanIPparts[0]) * 16777216 + Long.parseLong(scanIPparts[1]) * 65536 
	       				 + Long.parseLong(scanIPparts[2]) * 256 + Long.parseLong(scanIPparts[3]);
	        		
	        		if (intUniqueIP > intScanIP) {
	        			result = scanner.seekTo(Bytes.toBytes(line), intUniqueIP - intScanIP);
	        		}
	        		else {
	        			line = buffered.seekTo(Bytes.toString(result.getRow()));
	        		}
	        		
	        		if (line == null || result == null)
	        			break;
	        		
	        		if (Bytes.toString(result.getRow()).equals(line)) {
	        			//System.out.println("ip :" + Bytes.toString(result.getRow()));
		            	DnsMap.put(Bytes.toString(result.getRow()), Bytes.toString(result.getValue(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"))));
		            }
	        		
	            }
	            buffered.close();
	            
	            System.out.println("Size of hashmap: " + DnsMap.size());
	            table.close();
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
        	//System.out.println("input ip: " + ip);

        		
        	return DnsMap.get(ip);
	    }
		
		public String evaluate(int partNum, String splitPointsFile, String ipFrom, String ipTo) throws HiveException, IOException {
	        if (DnsMap == null) {
	            DnsMap = new HashMap <String, String>();
	            splitPointsList = new ArrayList <String>();
	            try {
	                BufferedReader lineReader = new BufferedReader(new FileReader(splitPointsFile));

	                String line = null;
	                while ((line = lineReader.readLine()) != null) {
	                    splitPointsList.add(line);
	                }
	                lineReader.close();
	            } catch (FileNotFoundException e) {
	                throw new HiveException(splitPointsFile + " doesn't exist");
	            } catch (IOException e) {
	                throw new HiveException("process file " + splitPointsFile + " failed, please check format");
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
	            
	            
	            Configuration conf = HBaseConfiguration.create();
	    	    conf.set("hbase.table.name", "rdns_2");
	            
	            HTable table = new HTable(conf, "rdns_2");
	            
	            Scan scan = new Scan();
	            scan.setCaching(20000);
	            scan.setCacheBlocks(false);
	            scan.setBatch(20000);
	            scan.addColumn(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"));
	            
	            
	            if (partNum / partition_no != 0 && (partNum / partition_no) != (partition_no - 1)) {
	            	scan.setStartRow(Bytes.toBytes(splitPointsList.get(partNum / partition_no - 1)));
	            	//System.out.println("StartRow: " + splitPointsList.get(partNum / partition_no - 1));
	            	scan.setStopRow(Bytes.toBytes(splitPointsList.get(partNum / partition_no)));
	            	//System.out.println("StopRow: " + splitPointsList.get(partNum / partition_no));
	            }
	            
	            if (partNum / partition_no == 0) {
	            	scan.setStopRow(Bytes.toBytes(splitPointsList.get(partNum / partition_no)));
	            	//System.out.println("StopRow: " + splitPointsList.get(partNum / partition_no));
	            }
	            if (partNum / partition_no == partition_no - 1) { 
	            	scan.setStartRow(Bytes.toBytes(splitPointsList.get(partition_no - 1)));
	            	//System.out.println("StartRow:" + splitPointsList.get(partition_no - 1));
	            }
	            
	            //int count = 0;
	            Path pt=new Path("hdfs://hadoop-master:9000/user/root/uniqueIPs/part-r-" + partNo + ".gz");
                FileSystem fs = FileSystem.get(new Configuration());
	            InputStream fileStream = fs.open(pt);
	            InputStream gzipStream = new GZIPInputStream(fileStream);
	            Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
	            FileScanner buffered = new FileScanner(new BufferedReader(decoder));
	            String line = buffered.next();
	            
	            ClientScanner scanner = (ClientScanner) table.getScanner(scan);
	            Result result = scanner.next();
	            
	            while (line != null && result != null) {
	            	String[] uniqueParts = line.split("\\.");
	        		String[] scanIPparts = Bytes.toString(result.getRow()).split("\\.");
	        			
	        		long intUniqueIP = Long.parseLong(uniqueParts[0]) * 16777216 + Long.parseLong(uniqueParts[1]) * 65536 
		        				 + Long.parseLong(uniqueParts[2]) * 256 + Long.parseLong(uniqueParts[3]);
	        		
	        		long intScanIP = Long.parseLong(scanIPparts[0]) * 16777216 + Long.parseLong(scanIPparts[1]) * 65536 
	       				 + Long.parseLong(scanIPparts[2]) * 256 + Long.parseLong(scanIPparts[3]);
	        		
	        		if (intUniqueIP > intScanIP) {
	        			result = scanner.seekTo(Bytes.toBytes(line), intUniqueIP - intScanIP);
	        		}
	        		else {
	        			line = buffered.seekTo(Bytes.toString(result.getRow()));
	        		}
	        		
	        		if (line == null || result == null)
	        			break;
	        		
	        		if (Bytes.toString(result.getRow()).equals(line)) {
	        			//System.out.println("ip :" + Bytes.toString(result.getRow()));
		            	DnsMap.put(Bytes.toString(result.getRow()), Bytes.toString(result.getValue(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"))));
		            }
	        		
	            }
	            
	            
	            if (partNum % partition_no != 0 && (partNum % partition_no) != (partition_no - 1)) {
	            	scan.setStartRow(Bytes.toBytes(splitPointsList.get(partNum % partition_no - 1)));
	            	//System.out.println("StartRow: " + splitPointsList.get(partNum % partition_no - 1));
	            	scan.setStopRow(Bytes.toBytes(splitPointsList.get(partNum % partition_no)));
	            	//System.out.println("StopRow: " + splitPointsList.get(partNum % partition_no));
	            }
	            
	            if (partNum % partition_no == 0) {
	            	scan.setStopRow(Bytes.toBytes(splitPointsList.get(partNum % partition_no)));
	            	//System.out.println("StopRow: " + splitPointsList.get(partNum % partition_no));
	            }
	            if (partNum % partition_no == partition_no - 1) {
	            	scan.setStartRow(Bytes.toBytes(splitPointsList.get(partNum % partition_no - 1)));
	            	//System.out.println("StartRow: " + splitPointsList.get(partNum % partition_no - 1));
	            }
	            
	            scanner = (ClientScanner) table.getScanner(scan);
	            result = scanner.next();
	            //count = 0;
	            
	            while (line != null && result != null) {
	            	String[] uniqueParts = line.split("\\.");
	        		String[] scanIPparts = Bytes.toString(result.getRow()).split("\\.");
	        			
	        		long intUniqueIP = Long.parseLong(uniqueParts[0]) * 16777216 + Long.parseLong(uniqueParts[1]) * 65536 
		        				 + Long.parseLong(uniqueParts[2]) * 256 + Long.parseLong(uniqueParts[3]);
	        		
	        		long intScanIP = Long.parseLong(scanIPparts[0]) * 16777216 + Long.parseLong(scanIPparts[1]) * 65536 
	       				 + Long.parseLong(scanIPparts[2]) * 256 + Long.parseLong(scanIPparts[3]);
	        		
	        		if (intUniqueIP > intScanIP) {
	        			result = scanner.seekTo(Bytes.toBytes(line), intUniqueIP - intScanIP);
	        		}
	        		else {
	        			line = buffered.seekTo(Bytes.toString(result.getRow()));
	        		}
	        		
	        		if (line == null || result == null)
	        			break;
	        		
	        		if (Bytes.toString(result.getRow()).equals(line)) {
	        			//System.out.println("ip :" + Bytes.toString(result.getRow()));
		            	DnsMap.put(Bytes.toString(result.getRow()), Bytes.toString(result.getValue(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"))));
		            }
	        		
	            }
	            buffered.close();
	            
	            System.out.println("Size of hashmap: " + DnsMap.size());
	            table.close();
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
        	
        	return DnsMap.get(ipFrom) + " " + DnsMap.get(ipTo);
	    }
}

