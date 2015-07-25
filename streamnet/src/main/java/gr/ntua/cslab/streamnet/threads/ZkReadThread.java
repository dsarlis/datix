package gr.ntua.cslab.streamnet.threads;

import backtype.storm.task.TopologyContext;
import gr.ntua.cslab.streamnet.zookeeper.SyncWorker;

public class ZkReadThread implements Runnable {
	private String zkHosts;
	private String stateRoot;
	private String lockRoot;
	private String tableName;
	private String boltName;
	private int waitTime;
	private TopologyContext topo;
	private int boltNo;
	
	public ZkReadThread(String zkHosts, String stateRoot, String lockRoot, 
			String tableName, String boltName, int waitTime, TopologyContext topo, int boltNo) {
		this.zkHosts = zkHosts;
		this.stateRoot = stateRoot;
		this.lockRoot = lockRoot;
		this.boltName = boltName;
		this.tableName = tableName;
		this.waitTime = waitTime;
		this.topo = topo;
		this.boltNo = boltNo;
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
		SyncWorker sw = new SyncWorker(zkHosts, 2000000, stateRoot, lockRoot, tableName, boltName, topo, boltNo);
//		System.out.println("Started reading K-d Tree and Mapping File into memory...");
		sw.blockingStateRead();
	}
}

