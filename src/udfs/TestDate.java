package diplom;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestDate {
	public static void main(String[] args) throws IOException, ParseException {
		    String strDate = "2014-08-16";
	        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	        Date dateStr = formatter.parse(strDate);
	        System.out.println(dateStr.getTime());
	}
}
