package gr.ntua.cslab.streamnet.threads;

import gr.ntua.cslab.streamnet.zookeeper.SyncWorker;

public class ZkReadThread implements Runnable {
	private String zkHosts;
	private String root;
	private String tableName;
	
	public ZkReadThread(String zkHosts, String root, String tableName) {
		this.zkHosts = zkHosts;
		this.root = root;
		this.tableName = tableName;
	}
	
	@Override
	public void run() {
		
		SyncWorker sw = new SyncWorker(zkHosts, root, tableName);
		if (root.equals("/datix")) {
			System.out.println("Starting reading K-d Tree and Mapping File into memory...");
			sw.read(true);
		}
	}
}

