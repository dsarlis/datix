package gr.ntua.cslab.streamnet.zookeeper;

import gr.ntua.cslab.streamnet.beans.State;
import gr.ntua.cslab.streamnet.cache.KDtreeCache;
import gr.ntua.cslab.streamnet.cache.LeafPointsCache;
import gr.ntua.cslab.streamnet.cache.MappingCache;
import gr.ntua.cslab.streamnet.kdtree.KdTree;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class SyncPrimitive implements Watcher {
	public static ZooKeeper zk = null;
    public static Integer mutex;
    public static boolean dead;
    public static int size;
    private String address;
    private int timeout;
    protected final String BOLT_NAME;
    private static final Logger LOG = Logger.getLogger(SyncPrimitive.class.getName());

    public String stateRoot;
    public String lockRoot;

    public SyncPrimitive(String address, int timeout, String boltName) {
    	this.address = address;
    	this.timeout = timeout;
    	this.BOLT_NAME = boltName;
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
    
    synchronized public void getState() {
    	Stat stat = null;
    	try {
    		List<String> list = zk.getChildren(stateRoot, true);
    		Collections.sort(list);
    		String last = list.get(list.size()-1);
		
    		LOG.info("FileList: " + list);
    		LOG.info("Chosen one: " + last);
		
    		//wait for all files to exist
    			
    		byte[] b = zk.getData(stateRoot + "/" + last, false, stat);
    		LOG.info("Mapping File read from Zookeeper");
    		ObjectInputStream o = new ObjectInputStream(new ByteArrayInputStream(b));
    		State state = (State) o.readObject();
    		o.close();
    		HashMap<String, String> newMapping = (HashMap<String, String>) state.getFileMapping();
    		if (MappingCache.getFileMapping() == null || newMapping.keySet().size() > MappingCache.getFileMapping().keySet().size())
    			MappingCache.setFileMapping(newMapping);
    		BufferedWriter bw = new BufferedWriter(new FileWriter("/tmp/mapping_dup"));
    		for (String key: MappingCache.getFileMapping().keySet()) {
    			bw.write("key: " + key + " value: " + MappingCache.getFileMapping().get(key));
    			bw.newLine();
    		}
    		bw.close();
			
    		LOG.info("Points File read from Zookeeper");
    		if (LeafPointsCache.getPoints() == null)
				LeafPointsCache.setPoints(new HashMap<Integer, ArrayList<double[]>>());
    		HashMap<Integer, ArrayList<double[]>> newPoints = 
				(HashMap<Integer, ArrayList<double[]>>) state.getPoints();
    		if (newPoints != null) {
    			for (int key: newPoints.keySet()) {
    				String worker = MappingCache.getFileMapping().get(""+key);
    				if (worker.equals(BOLT_NAME)) {
    					if (newPoints.get(key) != null) {
    						for (double[] point: newPoints.get(key)) {
    							LeafPointsCache.addPoint(key, point);
    						}
    					}
    				}
    			}
    			bw = new BufferedWriter(new FileWriter("/tmp/leaf_points"));
    			for (int key: LeafPointsCache.getPoints().keySet()) {
    				bw.write("key: " + key + " value: something not null");
    				bw.newLine();
    				for (double[] point: LeafPointsCache.getPoints().get(key)) {
    					bw.write("point: " + point[0] + "," + point[1] + "," + point[2]);
    					bw.newLine();
    				}
    			}
    			bw.close();
    		}
			
    		LOG.info("K-d Tree read from Zookeeper");
    		KdTree<Long> newKd = state.getKd();
    		if (KDtreeCache.getKd() == null || newKd.countLeafs() > KDtreeCache.getKd().countLeafs())
    			KDtreeCache.setKd(newKd);
    		bw = new BufferedWriter(new FileWriter("/tmp/kdtree_dup"));
    		KDtreeCache.getKd().printTree(bw);
    		bw.close();
    	} catch (KeeperException e) {
    		System.err.println("Keeper exception when trying to read data "
    				+ "from Zookeeper: " + e.toString());
    	} catch (InterruptedException e) {
    		System.err.println("Interrupted exception when trying to read data "
    				+ "from Zookeeper: " + e.toString());
    	} catch (IOException e) {
    		System.err.println("IO exception when trying to read data "
    				+ "from Zookeeper: " + e.toString());
    	} catch (ClassNotFoundException e) {
    		System.err.println(e.toString());
    	}
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
            		try {
            			mutex.notifyAll();
            			zk = new ZooKeeper(address, timeout, this);
            			mutex = new Integer(-1);
            			dead = false;
            		} catch (IOException e) {
            			e.printStackTrace();
            		}
            		break;
            	default:
            		break;
            } 	
        } else {
            if (path != null && (path.equals(stateRoot) || path.equals(lockRoot))) {
            	// The node has changed, so wake up workers
            	synchronized(mutex) {
            		mutex.notify();
            	}
            }
        }
    }
}
