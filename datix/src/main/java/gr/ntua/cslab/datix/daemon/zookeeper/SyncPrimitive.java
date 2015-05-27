package gr.ntua.cslab.datix.daemon.zookeeper;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class SyncPrimitive implements Watcher {
		public Logger logger = Logger.getLogger(SyncPrimitive.class);
	    public static ZooKeeper zk = null;
	    public static Integer mutex;
	    public static boolean dead = false;

	    public String root;

	    public SyncPrimitive(String address) {
	        if(zk == null){
	            try {
	                logger.info("Starting ZK:");
	                zk = new ZooKeeper(address, 3000, this);
	                mutex = new Integer(-1);
	                logger.info("Finished starting ZK: " + zk);
	            } catch (IOException e) {
	                logger.info(e.toString());
	                zk = null;
	            }
	        }
	        //else mutex = new Integer(-1);
	    }

	    synchronized public void process(WatchedEvent event) {
	    	String path = event.getPath();
	        if (event.getType() == Event.EventType.None) {
	            // We are are being told that the state of the
	            // connection has changed
	        	switch (event.getState()) {
	            	case SyncConnected:
	            		// In this particular example we don't need to do anything
	            		// here - watches are automatically re-registered with 
	            		// server and any watches triggered while the client was 
	            		// disconnected will be delivered (in order of course)
	            		break;
	            	case Expired:
	            		// It's all over
	            		dead = true;
	            		mutex.notifyAll();
	            		break;
	            	default:
	            		break;
	            } 	
	        } else {
	            if (path != null && path.equals(root)) {
	            	// The node has changed, so wake up workers
	            	synchronized(mutex) {
	            		mutex.notify();
	            	}
	            }
	        }
	    }
}
