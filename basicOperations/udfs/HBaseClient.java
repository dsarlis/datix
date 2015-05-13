package diplom;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
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

public class HBaseClient {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
	    conf.set("hbase.table.name", "rdns_2");
	    HashMap<String, String> DnsMap = new HashMap<String, String>();
        
        HTable table = new HTable(conf, "rdns_2");
        
        Scan scan = new Scan();
        scan.setCaching(20000);
        scan.setCacheBlocks(false);
        scan.setBatch(20000);
        scan.addColumn(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"));
        
        Path pt=new Path("hdfs://hadoop-master:9000/user/root/uniqueIPs/part-r-00899.gz");
        FileSystem fs = FileSystem.get(new Configuration());
        InputStream fileStream = fs.open(pt);
        InputStream gzipStream = new GZIPInputStream(fileStream);
        Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
        BufferedReader buffered = new BufferedReader(decoder);
        String line = buffered.readLine();
        
        scan.setStartRow(Bytes.toBytes("005.130.039.009"));
        scan.setStopRow(Bytes.toBytes("023.029.019.178"));
        //003.105.189.052
        ClientScanner scanner = (ClientScanner) table.getScanner(scan);
        Result result = scanner.next();
        
        try {
        	while (line != null) {
        			
        		while ((Bytes.toString(result.getRow())).compareTo(line) > 0) {
            		line = buffered.readLine();
            		if (line == null) {
	            		buffered.close();
	            		break;
	            	}
            	}
        		String[] uniqueParts = line.split("\\.");
        		String[] scanIPparts = Bytes.toString(result.getRow()).split("\\.");
        			
        		long intUniqueIP = Long.parseLong(uniqueParts[0]) * 16777216 + Long.parseLong(uniqueParts[1]) * 65536 
	        				 + Long.parseLong(uniqueParts[2]) * 256 + Long.parseLong(uniqueParts[3]);
        		
        		long intScanIP = Long.parseLong(scanIPparts[0]) * 16777216 + Long.parseLong(scanIPparts[1]) * 65536 
       				 + Long.parseLong(scanIPparts[2]) * 256 + Long.parseLong(scanIPparts[3]);
        			
        			
        		
        		result = scanner.seekTo(Bytes.toBytes(line), intUniqueIP - intScanIP);
        		if (result == null)
        			break;
        			
        			//System.out.println("key: " + Bytes.toString(result.getRow()) + ", value: " + Bytes.toString(result.getValue(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"))));
        			if (Bytes.toString(result.getRow()).equals(line)) {
        				System.out.println("ip :" + Bytes.toString(result.getRow()));
	            		DnsMap.put(Bytes.toString(result.getRow()), Bytes.toString(result.getValue(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"))));
	            		line = buffered.readLine();
	            		if (line == null) {
	            			buffered.close();
	            			break;
	            		}
	            	}
        		}
        }
        finally {
        	
        }
        
        scan.setStartRow(Bytes.toBytes("037.025.120.007"));
        scan.setStopRow(Bytes.toBytes("041.027.078.187"));
        //041.027.078.187
        
        scanner = (ClientScanner) table.getScanner(scan);
        result = scanner.next();
        try {
        	while (line != null) {
        		
        		while ((Bytes.toString(result.getRow())).compareTo(line) > 0) {
            		line = buffered.readLine();
            		if (line == null) {
	            		buffered.close();
	            		break;
	            	}
            	}
        		
        		String[] uniqueParts = line.split("\\.");
        		String[] scanIPparts = Bytes.toString(result.getRow()).split("\\.");
        			
        		long intUniqueIP = Long.parseLong(uniqueParts[0]) * 16777216 + Long.parseLong(uniqueParts[1]) * 65536 
	        				 + Long.parseLong(uniqueParts[2]) * 256 + Long.parseLong(uniqueParts[3]);
        		
        		long intScanIP = Long.parseLong(scanIPparts[0]) * 16777216 + Long.parseLong(scanIPparts[1]) * 65536 
       				 + Long.parseLong(scanIPparts[2]) * 256 + Long.parseLong(scanIPparts[3]);
        		
        		result = scanner.seekTo(Bytes.toBytes(line), intUniqueIP - intScanIP);
        		if (result == null)
        			break;
        		
    			//System.out.println("key: " + Bytes.toString(result.getRow()) + ", value: " + Bytes.toString(result.getValue(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"))));
        		if (Bytes.toString(result.getRow()).equals(line)) {
        			System.out.println("ip :" + Bytes.toString(result.getRow()));
            		DnsMap.put(Bytes.toString(result.getRow()), Bytes.toString(result.getValue(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns"))));
            		line = buffered.readLine();
            		if (line == null) {
            			buffered.close();
            			break;
            		}
            	}
        	}
        }
        finally {
        	scanner.close();
        	buffered.close();
        }
        
        table.close();
	}
}
