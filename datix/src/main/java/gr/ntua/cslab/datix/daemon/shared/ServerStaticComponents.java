package gr.ntua.cslab.datix.daemon.shared;

import java.util.concurrent.locks.Lock;

public class ServerStaticComponents {
	private static Lock lock; 
	private static String tableName;
	private static String[] slaves;
	private static String kdTreeFile;
	private static String hiveStoreLocation;
	private static String mappingFile;
	
	public static Lock getLock() {
		return lock;
	}
	public static void setLock(Lock lock) {
		ServerStaticComponents.lock = lock;
	}
	public static String getMappingFile() {
		return mappingFile;
	}
	public static void setMappingFile(String mappingFile) {
		ServerStaticComponents.mappingFile = mappingFile;
	}
	public static String getKdTreeFile() {
		return kdTreeFile;
	}
	public static void setKdTreeFile(String kdTreeFile) {
		ServerStaticComponents.kdTreeFile = kdTreeFile;
	}
	public static String getHiveStoreLocation() {
		return hiveStoreLocation;
	}
	public static void setHiveStoreLocation(String hiveStoreLocation) {
		ServerStaticComponents.hiveStoreLocation = hiveStoreLocation;
	}
	public static String getTableName() {
		return ServerStaticComponents.tableName;
	}
	public static void setTableName(String tableName) {
		ServerStaticComponents.tableName = tableName;
	}
	public static String[] getSlaves() {
		return slaves;
	}
	public static void setSlaves(String[] slaves) {
		ServerStaticComponents.slaves = slaves;
	}
}
