package gr.ntua.cslab.streamnet.threads;

import gr.ntua.cslab.streamnet.zookeeper.SyncWorker;

public class ZkReadThread implements Runnable {
	private String zkHosts;
	private String stateRoot;
	private String lockRoot;
	private String tableName;
	private String boltName;
	private int waitTime;
	
	public ZkReadThread(String zkHosts, String stateRoot, String lockRoot, 
			String tableName, String boltName, int waitTime) {
		this.zkHosts = zkHosts;
		this.stateRoot = stateRoot;
		this.lockRoot = lockRoot;
		this.boltName = boltName;
		this.tableName = tableName;
		this.waitTime = waitTime;
	}
	
	@Override
	public void run() {
		
		if (waitTime > 0) {
			try {
				Thread.sleep(waitTime);
			} catch (InterruptedException e) {
				System.out.println(e.toString());
			}
		}
		SyncWorker sw = new SyncWorker(zkHosts, 2000000, stateRoot, lockRoot, tableName, boltName);
		System.out.println("Started reading K-d Tree and Mapping File into memory...");
		sw.blockingStateRead();
	}
}

