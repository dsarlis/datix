package diplom;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;


public class GetCountry extends UDF{
		private TreeMap<Long, String> countryMap;

		public String evaluate(String ip, String mapFile) throws HiveException {
	        if (countryMap == null) {
	            countryMap = new TreeMap <Long, String>();
	            try {
	                BufferedReader lineReader = new BufferedReader(new FileReader(mapFile));

	                String line = null;
	                while ((line = lineReader.readLine()) != null) {
	                    String[] fields = line.split(",");
	                    long ipStart = Long.parseLong(fields[2].replace("\"", ""));
	                    long ipEnd = Long.parseLong(fields[3].replace("\"", ""));
	                    String countryCode = fields[4].replace("\"", "");
	                    String countryName = fields[5].replace("\"", "");
	                    countryMap.put(ipStart, countryCode + "_" + countryName + "_start");
	                    countryMap.put(ipEnd, countryCode + "_" + countryName + "_stop");
	                }
	                lineReader.close();
	            } catch (FileNotFoundException e) {
	                throw new HiveException(mapFile + " doesn't exist");
	            } catch (IOException e) {
	                throw new HiveException("process file " + mapFile + " failed, please check format");
	            }
	        }

	        String[] ipParts = ip.split("\\.");
	        if (ipParts.length > 1) {
	        	long intIP = Long.parseLong(ipParts[0]) * 16777216 + Long.parseLong(ipParts[1]) * 65536 
	        				 + Long.parseLong(ipParts[2]) * 256 + Long.parseLong(ipParts[3]);
	        	Long key = countryMap.ceilingKey(intIP);
	        	if (key != null) {
	        		if (key == intIP) {
	        			String[] part = countryMap.get(key).split("_");
	        			return part[0] + "_" + part[1];
	        		}
	        		else {
	        			String[] part = countryMap.get(key).split("_");
	        			if (part[2].equals("stop"))
	        				return part[0] + "_" + part[1];
	        		}
	        	}
	        }

	        return null;
	    }
}
