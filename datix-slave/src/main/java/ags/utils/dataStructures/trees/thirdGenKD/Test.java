package ags.utils.dataStructures.trees.thirdGenKD;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Test {
	public static void main(String[] args) throws Exception {
		KdTree<Long> kd = new KdTree<Long>(3, 5000);
		long start = System.currentTimeMillis();
		Random ran = new Random();
		for (int i = 0; i < 1000000; i++) {
			double[] point = new double[3];
			point[0]=ran.nextDouble();
			point[1]=ran.nextDouble();
			point[2]=ran.nextDouble();
			kd.addPoint(point, ran.nextLong());
			
		}
		long stop = System.currentTimeMillis();
		System.out.println((stop-start)/1000.0);
		//find partition
		int size = kd.countLeafs();
		System.out.println("partitions: "+size);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/tmp/filename.txt"), "utf-8"));
		    
		kd.printTree(writer);
		
		BufferedReader br = new BufferedReader(new FileReader("/tmp/filename.txt"));
		KdTree<Long> kd1 = new KdTree<Long>(br);
		size = kd1.countLeafs();
		System.out.println("partitions: "+size);
		//kd.write();
		//kd.prtintTree();
		for (int i = 0; i < 10; i++) {
			double[] point = new double[3];
			point[0]=ran.nextDouble();
			point[1]=ran.nextDouble();
			point[2]=ran.nextDouble();
			int id = kd.find(point);
			int id2 = kd1.find(point);
			System.out.println(id+" "+id2);
			if(id!=id2){
				System.out.println("error!!");
				System.exit(0);
			}
			
		}
		double[] min = new double[3];
		double[] max = new double[3];
		for (int i = 0; i < 1000; i++) {
			min[0]=ran.nextDouble();
			min[1]=ran.nextDouble();
			min[2]=ran.nextDouble();
			
			max[0]=min[0]+ran.nextDouble()/5.0;
			max[1]=min[1]+ran.nextDouble()/5.0;
			max[2]=min[2]+ran.nextDouble()/5.0;
			
			List<Integer> l = new ArrayList<Integer>();
			kd1.findBuckets(min, max,l);
			System.out.println(l);
			
		}
		
		
	}
}
