package gr.ntua.cslab.streamnet.cache;

import gr.ntua.cslab.streamnet.beans.SflowsList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class SFlowsCache {
		//caches sflows before writing them to HDFS
		private static Map<String, SflowsList> sflowsToStore ;
		
		//caches sflows before sending them to appropriate worker node
		private static Map<String, SflowsList> cachedSflows;

		public static synchronized Map<String, SflowsList> getSflowsToStore() {
			return sflowsToStore;
		}
		
		public static synchronized void setSflowsToStore(Map<String, SflowsList> sflowsToStore) {
			SFlowsCache.sflowsToStore = sflowsToStore;
		}

		public static synchronized Map<String, SflowsList> getCachedSflows() {
			return cachedSflows;
		}
		
		public static synchronized void setCachedSflows(Map<String, SflowsList> cachedSflows) {
			SFlowsCache.cachedSflows = cachedSflows;
		}
		
		public static synchronized void updateSflowsToStore(String key, SflowsList newList) {
			SflowsList sflowsList = SFlowsCache.sflowsToStore.get(key);
			if (sflowsList == null) {
				SFlowsCache.sflowsToStore.put(key, newList);
			}
			else {
				for (String value : newList.getSflowsList()) {
					sflowsList.updateList(value);
				}
			}
		}
		
		public static synchronized void deleteKeyFromSflowsToStore(String key) {
			SFlowsCache.sflowsToStore.remove(key);
		}
		
		public static synchronized void updateCachedSflows(String key, String value) {
			SflowsList sflowsList = SFlowsCache.cachedSflows.get(key);
			if (sflowsList == null) {
				SFlowsCache.cachedSflows.put(key, new SflowsList(new ArrayList<String>(Arrays.asList(value))));
			}
			else {
				sflowsList.updateList(value);
			}
		}
		
		public static synchronized boolean fullSflowsToStore() {
			boolean flag = false;
			int min = Integer.MAX_VALUE;
			int curr = Integer.MIN_VALUE;
			
			for (String key : SFlowsCache.sflowsToStore.keySet()) {
			    curr = SFlowsCache.sflowsToStore.get(key).getSflowsList().size();
				if (curr < min) {
					min = curr; 
				}
			}
			
			if (curr > 1000) {
				flag = true;
			}
			return flag;
		}
		
		public static synchronized boolean fullCachedSflows() {
			boolean flag = false;
			int min = Integer.MAX_VALUE;
			int curr = Integer.MIN_VALUE;
			
			for (String key : SFlowsCache.cachedSflows.keySet()) {
			    curr = SFlowsCache.cachedSflows.get(key).getSflowsList().size();
				if (curr < min) {
					min = curr; 
				}
			}
			
			if (curr > 1000) {
				flag = true;
			}
			return flag;
		}
}
