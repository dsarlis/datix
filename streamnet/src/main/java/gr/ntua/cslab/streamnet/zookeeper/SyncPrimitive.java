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
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import backtype.storm.task.TopologyContext;

public class SyncPrimitive implements Watcher {
	public static ZooKeeper zk = null;
    public static Integer mutex;
    public static boolean dead;
    public static int size;
    private String address;
    private int timeout;
    protected final String BOLT_NAME;
    protected TopologyContext topo;
    private static final Logger LOG = Logger.getLogger(SyncPrimitive.class.getName());

    public String stateRoot;
    public String lockRoot;

    public SyncPrimitive(String address, int timeout, String boltName, TopologyContext topo) {
    	this.address = address;
    	this.timeout = timeout;
    	this.BOLT_NAME = boltName;
    	this.topo = topo;
    
    	if(zk == null){
    		while (true) {
    			try {
    				System.out.println("Starting ZK:");
    				zk = new ZooKeeper(address, timeout, this);
    				mutex = new Integer(-1);
    				dead = false;
    				System.out.println("Finished starting ZK: " + zk);
    				return;
    			} catch (IOException e) {
    				LOG.info("!!!!" + e.toString());
    				e.printStackTrace();
    				zk = null;
    			}
    		}
    	}
        //else mutex = new Integer(-1);
    }
    
    synchronized public void getState() {
    	Stat stat = null;
    	while (true) {
    		try {
    			List<String> list = zk.getChildren(stateRoot, true);
    			Collections.sort(list);
    			String last = list.get(list.size()-1);
		
//    			LOG.info("FileList: " + list);
//    			LOG.info("Chosen one: " + last);
		
    			//wait for all files to exist
    			
    			byte[] b = zk.getData(stateRoot + "/" + last, false, stat);
//    			LOG.info("Mapping File read from Zookeeper");
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
			
//    			LOG.info("Points File read from Zookeeper");
    			if (LeafPointsCache.getPoints() == null)
    				LeafPointsCache.setPoints(new ConcurrentHashMap<Integer, ArrayList<double[]>>());
    			HashMap<Integer, ArrayList<double[]>> newPoints = 
    					(HashMap<Integer, ArrayList<double[]>>) state.getPoints();
    			if (newPoints != null) {
    				for (int key: newPoints.keySet()) {
    					int workerId = Integer.parseInt(MappingCache.getFileMapping().get(""+key));
//    					List<Integer> myList = topo.getComponentTasks(BOLT_NAME);
    					if (workerId == topo.getThisTaskId()) {
    						if (newPoints.get(key) != null) {
    							for (double[] point: newPoints.get(key)) {
    								LeafPointsCache.addPoint(key, point);
    							}
    						}
    					}
    				}
    			/*bw = new BufferedWriter(new FileWriter("/tmp/leaf_points"));
    			for (int key: LeafPointsCache.getPoints().keySet()) {
    				bw.write("key: " + key + " value: something not null");
    				bw.newLine();
    				for (double[] point: LeafPointsCache.getPoints().get(key)) {
    					bw.write("point: " + point[0] + "," + point[1] + "," + point[2]);
    					bw.newLine();
    				}
    			}
    			bw.close();*/
    			}
			
//    			LOG.info("K-d Tree read from Zookeeper");
    			KdTree<Long> newKd = state.getKd();
    			if (KDtreeCache.getKd() == null || newKd.countLeafs() > KDtreeCache.getKd().countLeafs())
    				KDtreeCache.setKd(newKd);
    			bw = new BufferedWriter(new FileWriter("/tmp/kdtree_dup"));
    			KDtreeCache.getKd().printTree(bw);
    			bw.close();
    			return;
    		} catch (KeeperException e) {
        		restartZK();
    			LOG.info("!!!Keeper exception when trying to read data "
    					+ "from Zookeeper: " + e.toString());
    			e.printStackTrace();
    		} catch (InterruptedException e) {
//        		restartZK();
    			LOG.info("!!!Interrupted exception when trying to read data "
    					+ "from Zookeeper: " + e.toString());
    			e.printStackTrace();
    		} catch (IOException e) {
//        		restartZK();
    			LOG.info("!!!IO exception when trying to read data "
    					+ "from Zookeeper: " + e.toString());
    			e.printStackTrace();
    		} catch (ClassNotFoundException e) {
    			LOG.info("!!!!" + e.toString());
    			e.printStackTrace();
    		}
    	}
    }

    public void restartZK() {
		LOG.info("Restarting zookeeper!! :)" );
			try {
				zk.close();
				zk = new ZooKeeper(address, timeout, this);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			dead = false;
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
            		synchronized(mutex) {
            			mutex.notifyAll();
            			restartZK();
            			mutex = new Integer(-1);
            		}
            		break;
            	case Disconnected:
            		synchronized (mutex) {
            			mutex.notifyAll();
            			restartZK();
            			mutex = new Integer(-1);
            		}
            		break;
            	default:
            		synchronized(mutex) {
            			mutex.notifyAll();
            			restartZK();
            			mutex = new Integer(-1);
            		}
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
