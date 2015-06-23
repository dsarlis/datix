package gr.ntua.cslab.streamnet.threads;

import gr.ntua.cslab.streamnet.zookeeper.SyncWorker;

public class SplitThread implements Runnable {
	private String zkHosts;
	private String root;
	private int id;
	private String tableName;
	private String boltName;
	
	public SplitThread(String zkHosts, String root, int id, 
			String tableName, String boltName) {
		this.zkHosts = zkHosts;
		this.root = root;
		this.id = id;
		this.tableName = tableName;
		this.boltName = boltName;
	}
	
	@Override
	public void run() {
		SyncWorker sw = new SyncWorker(zkHosts, root, 
				tableName, boltName);
		if (root.equals("/datix")) {
			System.out.println("Performing a split in Kd-Tree");
			sw.update(id);
		}		
	}
	
}
