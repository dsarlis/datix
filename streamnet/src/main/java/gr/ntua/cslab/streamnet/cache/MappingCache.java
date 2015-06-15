package gr.ntua.cslab.streamnet.cache;

import java.util.Map;

public class MappingCache {
		//mapping workers to files
		private static Map<String, String> fileMapping;
		
		public static synchronized Map<String, String> getFileMapping() {
			return fileMapping;
		}

		public static synchronized void setFileMapping(Map<String, String> fileMapping) {
			MappingCache.fileMapping = fileMapping;
		}
		
		public static synchronized void updateMapping(String key, String value) {
			MappingCache.fileMapping.put(key, value);
		}
		
		public static synchronized void updateMapping(String keyLeft, String valueLeft, String keyRight, String valueRight) {
			MappingCache.fileMapping.put(keyLeft, valueLeft);
			MappingCache.fileMapping.put(keyRight, valueRight);
		}
}
