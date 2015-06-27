package gr.ntua.cslab.streamnet.threads;

import gr.ntua.cslab.streamnet.zookeeper.BlockingRead;

public class ZkReadThread implements Runnable {
	private String zkHosts;
	private String root;
	private String boltName;
	private int waitTime;
	
	public ZkReadThread(String zkHosts, String root, String boltName, int waitTime) {
		this.zkHosts = zkHosts;
		this.root = root;
		this.boltName = boltName;
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
		BlockingRead br = new BlockingRead(zkHosts, 2000000, root, boltName);
		if (root.equals("/datix")) {
			System.out.println("Started reading K-d Tree and Mapping File into memory...");
			br.blockingRead();
		}
	}
}

