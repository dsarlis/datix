package diplom;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;


public class FilterIP extends UDF{
		private HashMap<String, String> IPMap;

		public String evaluate(String ipFrom, String ipTo, String mapFile) throws HiveException {
	        if (IPMap == null) {
	            IPMap = new HashMap <String, String>();
	            try {
	                BufferedReader lineReader = new BufferedReader(new FileReader(mapFile));

	                String line = null;
	                while ((line = lineReader.readLine()) != null) {
	                    String[] fields = line.split(" ");
	                    IPMap.put(fields[0], fields[1]);
	                }
	                lineReader.close();
	            } catch (FileNotFoundException e) {
	                throw new HiveException(mapFile + " doesn't exist");
	            } catch (IOException e) {
	                throw new HiveException("process file " + mapFile + " failed, please check format");
	            }
	        }

	        boolean src = IPMap.containsKey(ipFrom);
	        boolean dst = IPMap.containsKey(ipTo);
	        
	        if (src && dst)
	        	return "DARK";
	        else if (!src && dst)
	        	return "RX_SRV";
	        else if (src && !dst)
	        	return "TX_SRV";
	        else return "UNK_S2S";
	    }
}
