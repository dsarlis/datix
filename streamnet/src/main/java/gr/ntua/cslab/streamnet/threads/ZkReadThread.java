package gr.ntua.cslab.streamnet.threads;

import gr.ntua.cslab.streamnet.zookeeper.SyncWorker;

import org.apache.log4j.Logger;

public class ZkReadThread implements Runnable {
	public Logger logger = Logger.getLogger(ZkReadThread.class);
	public String zkHosts;
	public String root;
	
	public ZkReadThread(String zkHosts, String root) {
		this.zkHosts = zkHosts;
		this.root = root;
	}
	
	@Override
	public void run() {
		
		SyncWorker sw = new SyncWorker(zkHosts, root);
		if (root.equals("/datix")) {
			logger.info("Starting reading K-d Tree and Mapping File into memory...");
			sw.read(true);
		}
	}
}

