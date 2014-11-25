package diplom;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class GetLocid extends UDF{
	private TreeMap<Long, String> blockMap;

	public Long evaluate(String ip, String mapFile) throws HiveException {
        if (blockMap == null) {
            blockMap = new TreeMap <Long, String>();
            try {
                BufferedReader lineReader = new BufferedReader(new FileReader(mapFile));

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
                throw new HiveException(mapFile + " doesn't exist");
            } catch (IOException e) {
                throw new HiveException("process file " + mapFile + " failed, please check format");
            }
        }

        String[] ipParts = ip.split("\\.");
        if (ipParts.length > 1) {
        	long intIP = Long.parseLong(ipParts[0]) * 16777216 + Long.parseLong(ipParts[1]) * 65536 
        				 + Long.parseLong(ipParts[2]) * 256 + Long.parseLong(ipParts[3]);
        	Long key = blockMap.ceilingKey(intIP);
        	if (key != null) {
        		if (key == intIP)
        			return Long.parseLong(blockMap.get(key).split("_")[0]);
        		else {
        			String[] part = blockMap.get(key).split("_");
        			if (part[1].equals("stop"))
        				return Long.parseLong(part[0]);
        		}
        	}
        }

        return null;
    }
}
