package gr.ntua.cslab.streamnet.threads;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CopyThread implements Runnable {
	private int oldId;
	private int leftId;
	private int rightId;
	private double splitValue;
	private int splitDimension;
	private final String TABLE_NAME;

	public CopyThread(int oldId, int leftId, int rightId, double splitValue, 
			int splitDimension, String tableName) {
		super();
		this.oldId = oldId;
		this.leftId = leftId;
		this.rightId = rightId;
		this.splitDimension = splitDimension;
		this.splitValue = splitValue;
		TABLE_NAME = tableName;
	}

	@Override
	public void run() {
		System.out.println(oldId + " " + leftId + " " + rightId);
		
		Path pt = new Path("hdfs://master:9000/opt/warehouse/" + TABLE_NAME 
				+ "/part=" + oldId + "/part-" + oldId + ".gz");
		try {
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new BufferedReader(
        		new InputStreamReader(new GZIPInputStream(fs.open(pt)), "UTF-8")));
        Path ptLeft = new Path("hdfs://master:9000/opt/warehouse/" + TABLE_NAME 
        		+ "/part=" + leftId + "/part-" + leftId + ".gz");
//        fs.create(ptLeft);
    	BufferedWriter bwLeft = new BufferedWriter(new OutputStreamWriter(
    			new GZIPOutputStream(fs.create(ptLeft))));
    	Path ptRight = new Path("hdfs://master:9000/opt/warehouse/" + TABLE_NAME 
    			+ "/part=" + rightId + "/part-" + rightId + ".gz");
//    	fs.create(ptRight);
    	BufferedWriter bwRight = new BufferedWriter(new OutputStreamWriter(
    			new GZIPOutputStream(fs.create(ptRight))));
        String line;
        System.out.println("------->Splitvalue chosen: " + splitValue + " SplitDimension: " + splitDimension);
		while ((line = br.readLine()) != null) {
			String[] parts1 = line.split(" ");
			double value = 0;
			
			switch (splitDimension) {
				case 0:	value = Double.parseDouble(parts1[1]);
					break;
				case 1:	value = Double.parseDouble(parts1[3]);
				case 2:	SimpleDateFormat formatter = 
										new SimpleDateFormat("yyyy-MM-dd");
				 				try {
				 						Date dateStr = formatter.parse(parts1[8]);
				 						value = (double) dateStr.getTime();
				 				} catch (ParseException e) {
				 						e.printStackTrace();
				 				}
				 				break;
                     
				default: System.err.println("Dimension number " + splitDimension);
            		 			break;
			}
			if (value > splitValue) {
				bwRight.write(line);
				bwRight.newLine();
			}
			else {
				bwLeft.write(line);
				bwLeft.newLine();
			}
        }
    	br.close();
    	bwLeft.close();
    	bwRight.close();
		} catch (IOException e) {
			System.err.println(e.toString());
		}	
	}
}
