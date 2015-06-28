package gr.ntua.cslab.streamnet.zookeeper;

import java.util.logging.Logger;

public class BlockingRead extends SyncPrimitive {
	public static boolean isAlive = false;
	private static final Logger LOG = Logger.getLogger(BlockingRead.class.getName());
	
	public BlockingRead (String address, int timeout, String root, String boltName) {
        super(address, timeout, boltName);
        this.stateRoot = root;
	}
	
	public void blockingRead() {
		
		while (true) {
            synchronized (mutex) {
            	if (!dead) {
            		getState();
                	LOG.info("Going to wait for poll-reading");
                	try {
						mutex.wait();
					} catch (InterruptedException e) {
						LOG.info(e.toString());
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
