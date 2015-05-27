package gr.ntua.cslab.datixSlave.daemon.threads;

import gr.ntua.cslab.datixSlave.daemon.zookeeper.SyncWorker;

import org.apache.log4j.Logger;

public class ZookeeperThread implements Runnable {
	public Logger logger = Logger.getLogger(ZookeeperThread.class);
	public String zkHosts;
	public String root;
	
	public ZookeeperThread(String zkHosts, String root) {
		this.zkHosts = zkHosts;
		this.root = root;
	}
	
	@Override
	public void run() {
		
		SyncWorker sw = new SyncWorker(zkHosts, root);
		if (root.equals("/datix")) {
			logger.info("Starting reading K-d Tree and Mapping File into memory...");
			sw.read();
		}
		else {
			sw = new SyncWorker(zkHosts, root);
			sw.write();
			logger.info("Writen alive message");
			sw.isAlive();
		}
	}
}
