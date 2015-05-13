package diplom;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ags.utils.dataStructures.trees.thirdGenKD.KdTree;

public class TestPartition {
	public static void main(String[] args) throws IOException, ParseException {
		
		FileSystem fs = FileSystem.get(new Configuration());
		Path pt = new Path("hdfs://hadoop-master:9000/user/root/tree_partition_3D");
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(pt)));
        
        KdTree<Long> kd = new KdTree<Long>(reader);
        
        /*pt = new Path("hdfs://hadoop-master:9000/opt/warehouse/sflows/sflow.2013-10-10_11-16-09.3600sec.pcap.gz");
        InputStream fileStream = fs.open(pt);
        InputStream gzipStream = new GZIPInputStream(fileStream);
        Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
        BufferedReader br = new BufferedReader(decoder);
        String line = br.readLine();
        
        while (line != null) {
        	String[] parts = line.split(" ");
        	System.out.println("ok");
        	double[] point = new double[3];
        	point[0] = Double.parseDouble(parts[1]);
        	point[1] = Double.parseDouble(parts[3]);
        	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			Date dateStr = formatter.parse(parts[8]);
			point[2] = dateStr.getTime();
         	int id = kd.find(point);
         	System.out.println("id: " + id);
         	line = br.readLine();
        }*/
        
        double[] min = new double[3];
        double[] max = new double[3];
        min[0] = min[1] = 0;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		Date dateStr = formatter.parse("2014-01-20");
        min[2] = (double) dateStr.getTime();
        
        max[0] = max[1] = (double) Long.MAX_VALUE;
        dateStr = formatter.parse("2014-01-27");
        max[2] = (double) dateStr.getTime();
        
        List<Integer> l = new ArrayList<Integer>();
        kd.findBuckets(min, max, l);
		Collections.sort(l);
		String query = "WHERE ";
		for (int e : l) {
			query += "part='" + e + "' OR ";
		}
		query = query.substring(0, query.length()-4);
		System.out.println(query);
		System.out.println(l.size());
	}
}
