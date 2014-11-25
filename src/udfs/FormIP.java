package diplom;

import java.math.BigDecimal;

import org.apache.hadoop.hive.ql.exec.UDF;

public class FormIP extends UDF{
	public String evaluate(final String ip){
		
	    int intIP = new BigDecimal(ip).intValue();
	    return String.valueOf(intIP);
	}
	
}
