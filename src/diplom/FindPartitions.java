package diplom;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ags.utils.dataStructures.trees.thirdGenKD.KdTree;

public class FindPartitions {
	public static void main(String[] args) throws IOException, ParseException {
		
		if (args.length < 5) {
			System.out.println("Wrong number of arguments. Usage <tree_partition> <filter_no> <first_filter> " +
					"<first_filter_startValue> <first_filter_stopValue> ...");
			System.exit(-1);
		}

		int dimensions = 3;
		int filter_no = Integer.parseInt(args[1]);
		FileSystem fs = FileSystem.get(new Configuration());
		Path pt = new Path("hdfs://hadoop-master:9000/user/root/" + args[0]);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(pt)));
        
        KdTree<Long> kd = new KdTree<Long>(reader);
        double[] min = new double[dimensions];
        double[] max = new double[dimensions];
        for (int i = 0; i < dimensions; i++) {
        	min[i] = 0;
        	max[i] = (double) Long.MAX_VALUE;
        }
        
        for (int i = 0; i < filter_no; i++) {
        	int index = Integer.parseInt(args[2 + 3 * i]);
        	switch (index) {
        		case 0:  String[] startParts = args[3 + 3 * i].split("\\.");
        				 min[0] = (double) Long.parseLong(startParts[0]) * 16777216 + Long.parseLong(startParts[1]) * 65536 
        								   + Long.parseLong(startParts[2]) * 256 + Long.parseLong(startParts[3]);
        				 String[] stopParts = args[4 + 3 * i].split("\\.");
        				 max[0] = (double) Long.parseLong(stopParts[0]) * 16777216 + Long.parseLong(stopParts[1]) * 65536 
        								   + Long.parseLong(stopParts[2]) * 256 + Long.parseLong(stopParts[3]);
        			 	 break;
        		case 1:  String[] startParts1 = args[3 + 3 * i].split("\\.");
				 		 min[1] = (double) Long.parseLong(startParts1[0]) * 16777216 + Long.parseLong(startParts1[1]) * 65536 
				 				           + Long.parseLong(startParts1[2]) * 256 + Long.parseLong(startParts1[3]);
				 		 String[] stopParts1 = args[4 + 3 * i].split("\\.");
				 		 max[1] = (double) Long.parseLong(stopParts1[0]) * 16777216 + Long.parseLong(stopParts1[1]) * 65536 
				 				 		   + Long.parseLong(stopParts1[2]) * 256 + Long.parseLong(stopParts1[3]);
			 		 	 break;
        		case 2:  SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				 	 	 try {
				 	 		 Date dateStr = formatter.parse(args[3 + 3 * i]);
				 	 		 min[2] = (double) dateStr.getTime();
				 	 		 dateStr = formatter.parse(args[4 + 3 * i]);
				 	 		 max[2] = (double) dateStr.getTime();
				 	 	 } catch (ParseException e) {
				 	 		 // TODO Auto-generated catch block
				 	 		 e.printStackTrace();
				 	 	 }
				 	 	 break;     
        		default: System.err.println("Dimension number n");
        		 	 	 break;
        	}
        }
        
        List<Integer> l = new ArrayList<Integer>();
        kd.findBuckets(min, max, l);
		Collections.sort(l);
		String query = "WHERE ";
		for (int e : l) {
			query += "part='" + e + "' OR ";
		}
		query = query.substring(0, query.length()-4);
		System.out.println(query);
	}
}

