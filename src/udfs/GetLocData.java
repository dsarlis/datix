package diplom;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class GetLocData extends UDF{
	private TreeMap<Long, String> blockMap;
	private Hashtable<Long, String> locMap;

	public String evaluate(String ip, String blockFile, String locFile) throws HiveException {
        if (blockMap == null) {
            blockMap = new TreeMap <Long, String>();
            try {
                BufferedReader lineReader = new BufferedReader(new FileReader(blockFile));

                String line = null;
                while ((line = lineReader.readLine()) != null) {
                    String[] fields = line.split(",");
                    long ipStart = Long.parseLong(fields[0].replace("\"", ""));
                    long ipEnd = Long.parseLong(fields[1].replace("\"", ""));
                    String locid = fields[2].replace("\"", "");
                    blockMap.put(ipStart, locid + "_start");
                    blockMap.put(ipEnd, locid + "_stop");
                }
                lineReader.close();
            } catch (FileNotFoundException e) {
                throw new HiveException(blockFile + " doesn't exist");
            } catch (IOException e) {
                throw new HiveException("process file " + blockFile + " failed, please check format");
            }
            
            locMap = new Hashtable <Long, String>();
            try {
                BufferedReader lineReader = new BufferedReader(new FileReader(locFile));

                String line = null;
                while ((line = lineReader.readLine()) != null) {
                    String[] fields = line.split(",");
                    long locid = Long.parseLong(fields[0].replace("\"", ""));
                    String value = "";
                    for (int i = 1; i < fields.length; i++) {
                    	value += fields[i] + "_";
                    }
                    value.replace("\"", "");
                    value = value.substring(0, value.length()-1);
                    locMap.put(locid, value);
                }
                lineReader.close();
            } catch (FileNotFoundException e) {
                throw new HiveException(locFile + " doesn't exist");
            } catch (IOException e) {
                throw new HiveException("process file " + locFile + " failed, please check format");
            }
        }

        String[] ipParts = ip.split("\\.");
        if (ipParts.length > 1) {
        	long intIP = Long.parseLong(ipParts[0]) * 16777216 + Long.parseLong(ipParts[1]) * 65536 
        				 + Long.parseLong(ipParts[2]) * 256 + Long.parseLong(ipParts[3]);
        	Long key = blockMap.ceilingKey(intIP);
        	if (key != null) {
        		if (key == intIP)
        			return locMap.get(Long.parseLong(blockMap.get(key).split("_")[0]));
        		else {
        			String[] part = blockMap.get(key).split("_");
        			if (part[1].equals("stop"))
        				return locMap.get(Long.parseLong(part[0]));
        		}
        	}
        }

        return null;
    }
}
