package gr.ntua.cslab.streamnet.cache;

import gr.ntua.cslab.streamnet.kdtree.KdTree;

public class KDtreeCache {
	    //kdtree used by services
		private static KdTree<Long> kd;
		
		//dimensions used for interacting with the kdtree
		private static String[] dimensions = {"1", "3", "8"};
		private static int bucketSize = 15000;

		public static synchronized KdTree<Long> getKd() {
			return kd;
		}

		public static synchronized void setKd(KdTree<Long> kd) {
			KDtreeCache.kd = kd;
		}

		public static synchronized String[] getDimensions() {
			return dimensions;
		}

		public static synchronized void setDimensions(String[] dimensions) {
			KDtreeCache.dimensions = dimensions;
		}

		public static synchronized int getBucketSize() {
			return bucketSize;
		}

		public static synchronized void setBucketSize(int bucketSize) {
			KDtreeCache.bucketSize = bucketSize;
		}
}
