package gr.ntua.cslab.streamnet.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class SyncPrimitive implements Watcher {
	public static ZooKeeper zk = null;
    public static Integer mutex;
    public static boolean dead;
    public static int size;

    public String root;

    public SyncPrimitive(String address, int timeout) {
        if(zk == null){
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, timeout, this);
                mutex = new Integer(-1);
                dead = false;
                System.out.println("Finished starting ZK: " + zk);
            } catch (IOException e) {
            	System.err.println(e.toString());
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
            if (path != null && (path.equals(root) || path.equals("/lock"))) {
            	// The node has changed, so wake up workers
            	synchronized(mutex) {
            		mutex.notify();
            	}
            }
        }
    }
}
