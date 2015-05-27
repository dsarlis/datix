package gr.ntua.cslab.datix.daemon.zookeeper;

import gr.ntua.cslab.datix.daemon.cache.KDtreeCache;
import gr.ntua.cslab.datix.daemon.cache.MappingCache;
import ags.utils.dataStructures.trees.thirdGenKD.KdTree;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class WriteFiles extends SyncPrimitive {
	public static Logger logger = Logger.getLogger(WriteFiles.class);

	public WriteFiles (String address, String root) {
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
                logger.info("Keeper exception when instantiating Datix: "
                                + e.toString());
            } catch (InterruptedException e) {
                logger.info("Interrupted exception");
            }
        }
	}
	
	public boolean write() {
		
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
			logger.info("Keeper exception when writing data to "
					+ "Zookeeper: " + e.toString());
		} catch (InterruptedException e) {
			logger.info("Interrupted exception when writing data "
					+ "to Zookeeper: " + e.toString());
		} catch (IOException e) {
			logger.info("IO exception when trying to read Kd Tree "
					+ "and Mapping File: " + e.toString());
		}
		return false;
    }
	
	@SuppressWarnings("unchecked")
	public void read() throws InterruptedException, IOException, ClassNotFoundException {
		Stat stat = null;
		
		while (true) {
            synchronized (mutex) {
            	try {
            		List<String> list = zk.getChildren(root, true);
                	if (!dead) {
                		if (list.size() != 0) {
                			byte[] b = zk.getData(root + "/kdtree",
                						false, stat);
                			ObjectInputStream o = new ObjectInputStream(new ByteArrayInputStream(b));
                			KDtreeCache.setKd((KdTree<Long>) o.readObject());
                			BufferedWriter bw = new BufferedWriter(new FileWriter("/tmp/kdtree_dup"));
                			KDtreeCache.getKd().printTree(bw);
                		}
                		logger.info("Going to wait");
                		mutex.wait();
                	} else {
                		return;
                	}
            	} catch (KeeperException e) {
            		e.printStackTrace();
            	}
            }
        }
    }
}
