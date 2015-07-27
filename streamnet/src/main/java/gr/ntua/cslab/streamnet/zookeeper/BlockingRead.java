package gr.ntua.cslab.streamnet.zookeeper;

import java.util.logging.Logger;

import backtype.storm.task.TopologyContext;

public class BlockingRead extends SyncPrimitive {
	public static boolean isAlive = false;
	private static final Logger LOG = Logger.getLogger(BlockingRead.class.getName());
	
	public BlockingRead (String address, int timeout, String root, String boltName, TopologyContext topo) {
        super(address, timeout, boltName, topo);
        this.stateRoot = root;
	}
	
	public void blockingRead() {
		
		while (true) {
            synchronized (mutex) {
            	if (!dead) {
            		getState();
//                	LOG.info("Going to wait for poll-reading");
                	try {
						mutex.wait();
					} catch (InterruptedException e) {
						LOG.info(e.toString());
						e.printStackTrace();
					}
                	/*try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						LOG.info(e.toString());
					}*/
                } else {
                	return;
                }
            }
        }
	}
}
