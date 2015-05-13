package diplom;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;


public class GetDnsHBaseGet extends UDF{
		private HTable table = null;

		public String evaluate(String ip) throws HiveException, IOException {
	        
			if (table == null) {
				Configuration conf = HBaseConfiguration.create();
				conf.set("hbase.table.name", "rdns_2");
            
				table = new HTable(conf, "rdns_2");
			}

	        String[] ipParts = ip.split("\\.");
	        
	        if (ipParts.length > 1) {
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
        	
	        	Get g = new Get(Bytes.toBytes(ip));
	        	Result r = table.get(g);

	        	if (r != null)
	        		return Bytes.toString(r.getValue(Bytes.toBytes("dnsnames"), Bytes.toBytes("rdns")));
	        }
        	
        	return null;
	    }
}
