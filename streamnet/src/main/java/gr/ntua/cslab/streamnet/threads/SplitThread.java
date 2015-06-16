package gr.ntua.cslab.streamnet.threads;

import org.apache.log4j.Logger;

import gr.ntua.cslab.streamnet.zookeeper.SyncWorker;

public class SplitThread implements Runnable {

	private static final Logger logger = Logger.getLogger(ZkReadThread.class);
	public String zkHosts;
	public String root;
	public int id;
	
	public SplitThread(String zkHosts, String root, int id) {
		this.zkHosts = zkHosts;
		this.root = root;
		this.id = id;
	}
	
	@Override
	public void run() {
		SyncWorker sw = new SyncWorker(zkHosts, root);
		if (root.equals("/datix")) {
			logger.info("Performing a split in Kd-Tree");
			sw.update(id);
		}		
	}
	
}
