package gr.ntua.cslab.datixSlave.daemon.cache;

import gr.ntua.cslab.datixSlave.beans.SflowsList;
import gr.ntua.cslab.datixSlave.daemon.shared.SlaveStaticComponents;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class SFlowsCache {
	//caches sflows before writing them to HDFS
	private static Map<String, SflowsList> sflowsToStore ;
	
	//caches sflows before sending them to appropriate slave node
	private static Map<String, SflowsList> cachedSflows;

	public static synchronized Map<String, SflowsList> getSflowsToStore() {
		return sflowsToStore;
	}
	
	public static void setSflowsToStore(Map<String, SflowsList> sflowsToStore) {
		SFlowsCache.sflowsToStore = sflowsToStore;
	}

	public static synchronized Map<String, SflowsList> getCachedSflows() {
		return cachedSflows;
	}
	
	public static void setCachedSflows(Map<String, SflowsList> cachedSflows) {
		SFlowsCache.cachedSflows = cachedSflows;
	}
	
	public static void updateSflowsToStore(String key, SflowsList newList) {
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
	
	public static void deleteKeyFromSflowsToStore(String key) {
		SFlowsCache.sflowsToStore.remove(key);
	}
	
	public static void updateCachedSflows(String key, String value) {
		SflowsList sflowsList = SFlowsCache.cachedSflows.get(key);
		if (sflowsList == null) {
			SFlowsCache.cachedSflows.put(key, new SflowsList(new ArrayList<String>(Arrays.asList(value))));
		}
		else {
			sflowsList.updateList(value);
		}
	}
	
	public static boolean fullSflowsToStore() {
		boolean flag = false;
		int min = Integer.MAX_VALUE;
		int curr = Integer.MIN_VALUE;
		
		for (String key : SFlowsCache.sflowsToStore.keySet()) {
		    curr = SFlowsCache.sflowsToStore.get(key).getSflowsList().size();
			if (curr < min) {
				min = curr; 
			}
		}
		
		if (curr > SlaveStaticComponents.getLimitForSflowsToStore()) {
			flag = true;
		}
		return flag;
//		return SFlowsCache.sflowsToStore.size() == 100;
	}
	
	public static boolean fullCachedSflows() {
		boolean flag = false;
		int min = Integer.MAX_VALUE;
		int curr = Integer.MIN_VALUE;
		
		for (String key : SFlowsCache.cachedSflows.keySet()) {
		    curr = SFlowsCache.cachedSflows.get(key).getSflowsList().size();
			if (curr < min) {
				min = curr; 
			}
		}
		
		if (curr > SlaveStaticComponents.getLimitForCachedSflows()) {
			flag = true;
		}
		return flag;
//		return SFlowsCache.cachedSflows.size() == 100;
	}
}
