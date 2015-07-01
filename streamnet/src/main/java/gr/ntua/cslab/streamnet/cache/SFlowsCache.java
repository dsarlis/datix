package gr.ntua.cslab.streamnet.cache;

import gr.ntua.cslab.streamnet.beans.SflowsList;
import gr.ntua.cslab.streamnet.shared.StreamNetStaticComponents;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class SFlowsCache {
		//caches sflows before writing them to HDFS
		private static Map<Integer, SflowsList> sflowsToStore ;
		
		//caches sflows before sending them to appropriate worker node
		private static Map<Integer, String> cachedSflows;

		public static synchronized Map<Integer, SflowsList> getSflowsToStore() {
			return sflowsToStore;
		}
		
		public static synchronized void setSflowsToStore(Map<Integer, SflowsList> sflowsToStore) {
			SFlowsCache.sflowsToStore = sflowsToStore;
		}

		public static synchronized Map<Integer, String> getCachedSflows() {
			return cachedSflows;
		}
		
		public static synchronized void setCachedSflows(Map<Integer, String> cachedSflows) {
			SFlowsCache.cachedSflows = cachedSflows;
		}
		
		public static synchronized void updateSflowsToStore(int key, String value) {
			SflowsList sflowsList = SFlowsCache.sflowsToStore.get(key);
			if (sflowsList == null) {
				SFlowsCache.sflowsToStore.put(key, new SflowsList(
						new ArrayList<String>(Arrays.asList(value))));
			}
			else {
					sflowsList.updateList(value);
			}
		}
		
		public static synchronized void cleanSflowToStore(ArrayList<Integer> keysRemoved) {
			for (int key: keysRemoved) {
				SFlowsCache.sflowsToStore.remove(key);
			}
		}
		
		public static synchronized void updateCachedSflows(int key, String value) {
			String records = SFlowsCache.cachedSflows.get(key);
			if (records == null) {
				SFlowsCache.cachedSflows.put(key, value);
			}
			else {
				records += "," + value;
				SFlowsCache.cachedSflows.put(key, records);
			}
		}
		
		public static synchronized boolean fullSflowsToStore() {
			boolean flag = false;
			int min = Integer.MAX_VALUE;
			int curr = Integer.MIN_VALUE;
			
			for (int key : SFlowsCache.sflowsToStore.keySet()) {
			    curr = SFlowsCache.sflowsToStore.get(key).getSflowsList().size();
				if (curr < min) {
					min = curr; 
				}
			}
			
			if (curr >2500) {
				flag = true;
			}
			return flag;
		}
		
		public static synchronized boolean fullCachedSflows() {
			boolean flag = false;
			int min = Integer.MAX_VALUE;
			int curr = Integer.MIN_VALUE;
			
			for (int key : SFlowsCache.cachedSflows.keySet()) {
			    curr = SFlowsCache.cachedSflows.get(key).split(",").length;
				if (curr < min) {
					min = curr; 
				}
			}
			
			if (curr > 200) {
				flag = true;
			}
			return flag;
		}
}
