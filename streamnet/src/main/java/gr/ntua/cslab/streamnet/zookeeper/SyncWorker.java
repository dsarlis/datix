package gr.ntua.cslab.streamnet.zookeeper;

import gr.ntua.cslab.streamnet.cache.KDtreeCache;
import gr.ntua.cslab.streamnet.cache.MappingCache;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import ags.utils.dataStructures.trees.thirdGenKD.KdTree;

public class SyncWorker extends SyncPrimitive {
	private static final Logger logger = Logger.getLogger(SyncWorker.class);
	public static boolean isAlive = false;
	
	public SyncWorker (String address, String root) {
        super(address);
        this.root = root;

        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {
                logger.error("Keeper exception when instantiating Datix: "
                                + e.toString());
            } catch (InterruptedException e) {
                logger.error("Interrupted exception");
            }
        }
	}
	
	private boolean write() {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o;
		try {
			o = new ObjectOutputStream(b);
			o.writeObject(KDtreeCache.getKd());
			byte[] kdtree = b.toByteArray();
			b = new ByteArrayOutputStream();
			o = new ObjectOutputStream(b);
			o.writeObject(MappingCache.getFileMapping());
			byte[] mapping = b.toByteArray();
				
			Stat s = null;
			s = zk.exists(root + "/kdtree", false);
			if (s != null) {
	        	zk.delete(root + "/kdtree", 0);
	        }
	        s = zk.exists(root + "/mapping", false);
	        if (s != null) {
	        	zk.delete(root + "/maping", 0);
	        }
	        zk.create(root + "/kdtree", kdtree, Ids.OPEN_ACL_UNSAFE, 
	        			CreateMode.PERSISTENT);
	        logger.info("K-d Tree written to Zookeeper");
	        zk.create(root + "/mapping", mapping, Ids.OPEN_ACL_UNSAFE, 
	    			CreateMode.PERSISTENT);
	        logger.info("Mapping File written to Zookeeper");
	        
	        return true;
		} catch (KeeperException e) {
			logger.error("Keeper exception when writing data to "
					+ "Zookeeper: " + e.toString());
		} catch (InterruptedException e) {
			logger.error("Interrupted exception when writing data "
					+ "to Zookeeper: " + e.toString());
		} catch (IOException e) {
			logger.error("IO exception when trying to read Kd Tree "
					+ "and Mapping File: " + e.toString());
		}
		return false;
	}
	
	@SuppressWarnings("unchecked")
	public void read(boolean blocking) {
		Stat stat = null;
		
		while (true) {
            synchronized (mutex) {
            	try {
            		List<String> list = zk.getChildren(root, true);
                	if (!dead) {
                		if (list.size() == 2) {
                			byte[] b = zk.getData(root + "/kdtree",
                						false, stat);
                			if (!blocking)
                				zk.delete(root + "/kdtree", 0);
                			logger.info("K-d Tree read from Zookeeper");
                			ObjectInputStream o = new ObjectInputStream(
                					new ByteArrayInputStream(b));
                			KDtreeCache.setKd((KdTree<Long>) o.readObject());
                			BufferedWriter bw = new BufferedWriter(new 
                					FileWriter("/tmp/kdtree_dup"));
                			KDtreeCache.getKd().printTree(bw);
                			o.close();
                			bw.close();
                			
                			b = zk.getData(root + "/mapping", false, stat);
                			if (!blocking)
                				zk.delete(root + "mapping", 0);
                			logger.info("Mapping File read from Zookeeper");
                			o = new ObjectInputStream(new ByteArrayInputStream(b));
                			MappingCache.setFileMapping((HashMap<String, String>) o.readObject());
                			ObjectOutputStream s = new ObjectOutputStream(new 
                					FileOutputStream("/tmp/mapping_dup"));
                			s.writeObject(MappingCache.getFileMapping());
                			o.close();
                			s.close();
                		}
                		if (blocking) {
                			logger.info("Going to wait");
                			mutex.wait();
                		}
                	} else {
                		return;
                	}
            	} catch (KeeperException e) {
            		logger.error("Keeper exception when trying to read data "
            				+ "from Zookeeper: " + e.toString());
            	} catch (InterruptedException e) {
            		logger.error("Interrupted exception when trying to read data "
            				+ "from Zookeeper: " + e.toString());
				} catch (IOException e) {
            		logger.error("IO exception when trying to read data "
            				+ "from Zookeeper: " + e.toString());
				} catch (ClassNotFoundException e) {
            		logger.error(e.toString());
				}
            }
        }
    }
	
	public boolean update(int id) {
		// update KDtreeCache, MappingCache, LeafPointsCache
		read(false);
		double[] splitResult = KDtreeCache.getKd().performSplit(id);
		if ((int) splitResult[0] != -1) {
			//TODO load balance partitions among workers
			MappingCache.updateMapping("" + (int)splitResult[1], "worker1", ""+(int) splitResult[2], "worker2");
			// update zookeeper views
			return write();
		}
		else
			return false;
	}
	
	public void isAlive() {	
		while (true) {
			synchronized(mutex) {
				try {
					List<String> list = zk.getChildren(root, true);
					if (!isAlive) {
						if (list.size() == 3) {
							isAlive = true;
							logger.info("Everyone is up. Going to wait...");
							mutex.wait();
						}
						else {
							logger.info("Some workers are down");
						}
					}
					else {
						logger.info("Everyone is up. Going to wait...");
						mutex.wait();
					}
				} catch (KeeperException e) {
					
				} catch (InterruptedException e) {
					
				}
			}
		}
	}
}
