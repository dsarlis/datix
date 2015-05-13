package gr.ntua.cslab.datix.daemon.cache;

import ags.utils.dataStructures.trees.thirdGenKD.KdTree;

public class KDtreeCache {
	 //kdtree used by services
	private static KdTree<Long> kd;
		
	//dimensions used for interacting with the kdtree
	private static String[] dimensions = {"1", "3", "8"};
	private static int bucketSize = 15000;

	public static KdTree<Long> getKd() {
		return kd;
	}

	public static void setKd(KdTree<Long> kd) {
		KDtreeCache.kd = kd;
	}

	public static String[] getDimensions() {
		return dimensions;
	}

	public static void setDimensions(String[] dimensions) {
		KDtreeCache.dimensions = dimensions;
	}
	
	public static int getBucketSize() {
		return bucketSize;
	}

	public static void setBucketSize(int bucketSize) {
		KDtreeCache.bucketSize = bucketSize;
	}
}
