package gr.ntua.cslab.datixSlave.daemon.zookeeper;

import gr.ntua.cslab.datixSlave.daemon.cache.KDtreeCache;
import gr.ntua.cslab.datixSlave.daemon.cache.MappingCache;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
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

public class Read extends SyncPrimitive {
	public static Logger logger = Logger.getLogger(Read.class);
	
	public Read (String address, String root) {
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
                			logger.info("Mapping File read from Zookeeper");
                			o = new ObjectInputStream(new ByteArrayInputStream(b));
                			MappingCache.setFileMapping((HashMap<String, String>) o.readObject());
                			ObjectOutputStream s = new ObjectOutputStream(new 
                					FileOutputStream("/tmp/mapping_dup"));
                			s.writeObject(MappingCache.getFileMapping());
                			o.close();
                			s.close();
                		}
                		logger.info("Going to wait");
                		mutex.wait();
                	} else {
                		return;
                	}
            	} catch (KeeperException e) {
            		logger.info("Keeper exception when trying to read data "
            				+ "from Zookeeper: " + e.toString());
            	}
            }
        }
    }
}
