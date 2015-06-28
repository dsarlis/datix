package gr.ntua.cslab.streamnet.threads;

import gr.ntua.cslab.streamnet.zookeeper.SyncWorker;

public class SplitThread implements Runnable {
	private String zkHosts;
	private String stateRoot;
	private String lockRoot;
	private int id;
	private String tableName;
	private String boltName;
	
	public SplitThread(String zkHosts, String stateRoot, String lockRoot, int id, 
			String tableName, String boltName) {
		this.zkHosts = zkHosts;
		this.stateRoot = stateRoot;
		this.lockRoot = lockRoot;
		this.id = id;
		this.tableName = tableName;
		this.boltName = boltName;
	}
	
	@Override
	public void run() {
		SyncWorker sw = new SyncWorker(zkHosts, 3000, stateRoot, lockRoot, 
				tableName, boltName);
		System.out.println("Performing a split in Kd-Tree");
		sw.update(id);		
	}
	
}
