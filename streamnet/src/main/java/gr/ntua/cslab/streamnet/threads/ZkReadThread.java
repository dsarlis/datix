package gr.ntua.cslab.streamnet.threads;

import gr.ntua.cslab.streamnet.zookeeper.SyncWorker;

public class ZkReadThread implements Runnable {
	private String zkHosts;
	private String root;
	private String tableName;
	private String boltName;
	
	public ZkReadThread(String zkHosts, String root, String tableName, String boltName) {
		this.zkHosts = zkHosts;
		this.root = root;
		this.tableName = tableName;
		this.boltName = boltName;
	}
	
	@Override
	public void run() {
		
		SyncWorker sw = new SyncWorker(zkHosts, 2000000, root, tableName, boltName);
		if (root.equals("/datix")) {
			System.out.println("Started reading K-d Tree and Mapping File into memory...");
			sw.blockingRead();
		}
	}
}

