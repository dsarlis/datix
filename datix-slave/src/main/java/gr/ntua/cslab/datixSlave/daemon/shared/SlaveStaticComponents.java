package gr.ntua.cslab.datixSlave.daemon.shared;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;

public class SlaveStaticComponents {
		private static Lock lock;
		private static Lock storeLock;
		private static ArrayList<Thread> workers;
		private static String kdTreeFile;
		private static String mappingFile;
		private static boolean isLocked;
		private static String tableName;
		private static int limitForCachedSflows = 5000;
		private static int limitForSflowsToStore = 5000;
	
		public static int getLimitForCachedSflows() {
			return limitForCachedSflows;
		}
		public static void setLimitForCachedSflows(int limitForCachedSflows) {
			SlaveStaticComponents.limitForCachedSflows = limitForCachedSflows;
		}
		public static int getLimitForSflowsToStore() {
			return limitForSflowsToStore;
		}
		public static void setLimitForSflowsToStore(int limitForSflowsToStore) {
			SlaveStaticComponents.limitForSflowsToStore = limitForSflowsToStore;
		}
		public static Lock getStoreLock() {
			return storeLock;
		}
		public static void setStoreLock(Lock storeLock) {
			SlaveStaticComponents.storeLock = storeLock;
		}
		public static String getTableName() {
			return tableName;
		}
		public static void setTableName(String tableName) {
			SlaveStaticComponents.tableName = tableName;
		}
		public static boolean isLocked() {
			return isLocked;
		}
		public static void setLocked(boolean isLocked) {
			SlaveStaticComponents.isLocked = isLocked;
		}
		public static String getMappingFile() {
			return mappingFile;
		}
		public static void setMappingFile(String mappingFile) {
			SlaveStaticComponents.mappingFile = mappingFile;
		}
		public static String getKdTreeFile() {
			return kdTreeFile;
		}
		public static void setKdTreeFile(String kdTreeFile) {
			SlaveStaticComponents.kdTreeFile = kdTreeFile;
		}
		public static Lock getLock() {
			return lock;
		}
		public static void setLock(Lock lock) {
			SlaveStaticComponents.lock = lock;
		}
		public static ArrayList<Thread> getWorkers() {
			return workers;
		}
		public static void setWorkers(ArrayList<Thread> workers) {
			SlaveStaticComponents.workers = workers;
		}	
}
