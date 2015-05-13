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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;


public class GetDns extends UDF{
		private ArrayList<String> splitPointsList;
		private HashMap<String, String> DnsMap;
		private int partition_no = 148;

		public String evaluate(int partNum, String ip, String splitPointsFile) throws HiveException, IOException {
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
	            	System.out.println("StartRow: " + splitPointsList.get(partNum / partition_no - 1));
	            	scan.setStopRow(Bytes.toBytes(splitPointsList.get(partNum / partition_no)));
	            	System.out.println("StopRow: " + splitPointsList.get(partNum / partition_no));
	            }
	            
	            if (partNum / partition_no == 0) {
	            	scan.setStopRow(Bytes.toBytes(splitPointsList.get(partNum / partition_no)));
	            	System.out.println("StopRow: " + splitPointsList.get(partNum / partition_no));
	            }
	            if (partNum / partition_no == partition_no - 1) { 
	            	scan.setStartRow(Bytes.toBytes(splitPointsList.get(partition_no - 1)));
	            	System.out.println("StartRow:" + splitPointsList.get(partition_no - 1));
	            }
	            
	            //int count = 0;
	            Path pt=new Path("hdfs://hadoop-master:9000/user/root/uniqueIPs/part-r-" + partNo + ".gz");
                FileSystem fs = FileSystem.get(new Configuration());
	            InputStream fileStream = fs.open(pt);
	            InputStream gzipStream = new GZIPInputStream(fileStream);
	            Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
	            BufferedReader buffered = new BufferedReader(decoder);
	            String line = buffered.readLine();
	            
	            ResultScanner scanner = table.getScanner(scan);
	            
	            for (Result result = scanner.next(); result != null; result = scanner.next()) {
	            	/*if (count % 100000 == 0) {
	            		System.out.println(count);
	            		System.out.println("key: " + Bytes.toString(result.getRow()) + ", value: " + Bytes.toString(result.getValue(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"))));
	            	}*/
	            	if (line == null)
	            		break;
	            	
	            	while ((Bytes.toString(result.getRow())).compareTo(line) > 0) {
	            		line = buffered.readLine();
	            		if (line == null) {
		            		buffered.close();
		            		break;
		            	}
	            	}
	            	
	            	if (Bytes.toString(result.getRow()).equals(line)) {
	            		DnsMap.put(Bytes.toString(result.getRow()), Bytes.toString(result.getValue(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"))));
	            		line = buffered.readLine();
	            		if (line == null) {
	            			buffered.close();
	            			break;
	            		}
	            	}
	            	//count++;
	            }
	            
	            if (partNum % partition_no != 0 && (partNum % partition_no) != (partition_no - 1)) {
	            	scan.setStartRow(Bytes.toBytes(splitPointsList.get(partNum % partition_no - 1)));
	            	System.out.println("StartRow: " + splitPointsList.get(partNum % partition_no - 1));
	            	scan.setStopRow(Bytes.toBytes(splitPointsList.get(partNum % partition_no)));
	            	System.out.println("StopRow: " + splitPointsList.get(partNum % partition_no));
	            }
	            
	            if (partNum % partition_no == 0) {
	            	scan.setStopRow(Bytes.toBytes(splitPointsList.get(partNum % partition_no)));
	            	System.out.println("StopRow: " + splitPointsList.get(partNum % partition_no));
	            }
	            if (partNum % partition_no == partition_no - 1) {
	            	scan.setStartRow(Bytes.toBytes(splitPointsList.get(partNum % partition_no - 1)));
	            	System.out.println("StartRow: " + splitPointsList.get(partNum % partition_no - 1));
	            }
	            
	            scanner = table.getScanner(scan);
	            //count = 0;
	            for (Result result = scanner.next(); result != null; result = scanner.next()) {
	            	/*if (count % 100000 == 0) {
	            		System.out.println("!" + count);
	            		System.out.println("!!!key: " + Bytes.toString(result.getRow()) + ", value: " + Bytes.toString(result.getValue(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"))));
	            	}*/
	            	if (line == null)
	            		break;
	            	
	            	while ((Bytes.toString(result.getRow())).compareTo(line) > 0) {
	            		line = buffered.readLine();
	            		if (line == null) {
	            			buffered.close();
	            			break;
	            		}
	            	}
	            		
	            	if (Bytes.toString(result.getRow()).equals(line)) {
	            		DnsMap.put(Bytes.toString(result.getRow()), Bytes.toString(result.getValue(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"))));
	            		line = buffered.readLine();
	            		if (line == null) {
	            			buffered.close();
	            			break;
	            		}
	            	}
	            	//count++;
	            }
	            
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
}

