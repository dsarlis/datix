package gr.ntua.cslab.streamnet.zookeeper;

import gr.ntua.cslab.streamnet.cache.KDtreeCache;
import gr.ntua.cslab.streamnet.cache.MappingCache;
import gr.ntua.cslab.streamnet.kdtree.KdTree;
import gr.ntua.cslab.streamnet.shared.StreamNetStaticComponents;
import gr.ntua.cslab.streamnet.threads.CopyThread;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class SyncWorker extends SyncPrimitive {
	public static boolean isAlive = false;
	private final String TABLE_NAME;
	private static final Logger LOG = Logger.getLogger(SyncWorker.class.getName());
	
	public SyncWorker (String address, String root, String tableName) {
        super(address);
        this.root = root;
        TABLE_NAME = tableName;

        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {
            	System.err.println("Keeper exception when instantiating Datix: "
                                + e.toString());
            } catch (InterruptedException e) {
            	System.err.println("Interrupted exception");
            }
        }
	}
	
	private Map<String,String> executeCommand(String[] command) throws IOException, InterruptedException {
		String c="Executing command: ";
		for (int i = 0; i < command.length; i++) {
			c+=command[i]+" ";
		}
		LOG.info(c);
		
		StringBuffer output = new StringBuffer();
		ProcessBuilder p = new ProcessBuilder(command);
		Process p1 = p.start();
		//Process p = Runtime.getRuntime().exec(command);
		p1.waitFor();
		Map<String,String> ret = new HashMap<String, String>();
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(p1.getInputStream()));
		String line = "";			
		while ((line = reader.readLine())!= null) {
			output.append(line + "\n");
		}
        LOG.info("Command Output: "+output.toString());
        ret.put("output", output.toString());
		reader = new BufferedReader(new InputStreamReader(p1.getErrorStream()));
		line = "";	
		output = new StringBuffer();		
		while ((line = reader.readLine())!= null) {
			output.append(line + "\n");
		}
        LOG.info("Command Error: "+output.toString());
        ret.put("error", output.toString());
		return ret;
 
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
	        LOG.info("K-d Tree written to Zookeeper");
	        zk.create(root + "/mapping", mapping, Ids.OPEN_ACL_UNSAFE, 
	    			CreateMode.PERSISTENT);
	        LOG.info("Mapping File written to Zookeeper");
	        
	        return true;
		} catch (KeeperException e) {
			System.err.println("Keeper exception when writing data to "
					+ "Zookeeper: " + e.toString());
		} catch (InterruptedException e) {
			System.err.println("Interrupted exception when writing data "
					+ "to Zookeeper: " + e.toString());
		} catch (IOException e) {
			System.err.println("IO exception when trying to read Kd Tree "
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
                			LOG.info("K-d Tree read from Zookeeper");
                			ObjectInputStream o = new ObjectInputStream(
                					new ByteArrayInputStream(b));
                			KdTree<Long> newKd = (KdTree<Long>) o.readObject();
                			if (newKd.countLeafs() >= KDtreeCache.getKd().countLeafs())
                				KDtreeCache.setKd(newKd);
                			BufferedWriter bw = new BufferedWriter(new 
                					FileWriter("/tmp/kdtree_dup"));
                			KDtreeCache.getKd().printTree(bw);
                			o.close();
                			bw.close();
                			
                			b = zk.getData(root + "/mapping", false, stat);
                			if (!blocking)
                				zk.delete(root + "mapping", 0);
                			LOG.info("Mapping File read from Zookeeper");
                			o = new ObjectInputStream(new ByteArrayInputStream(b));
                			MappingCache.setFileMapping((HashMap<String, String>) o.readObject());
                			ObjectOutputStream s = new ObjectOutputStream(new 
                					FileOutputStream("/tmp/mapping_dup"));
                			s.writeObject(MappingCache.getFileMapping());
                			o.close();
                			s.close();
                		}
                		else {
                			LOG.info("Going to wait");
                			mutex.wait();
                		}
                		if (blocking) {
                			LOG.info("Going to wait");
                			mutex.wait();
                		}
                	} else {
                		return;
                	}
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
        }
    }
	
	public boolean update(int id) {
		// update KDtreeCache, MappingCache, LeafPointsCache
		read(false);
		double[] splitResult = KDtreeCache.getKd().performSplit(id);
		LOG.info("********************");
		LOG.info("Split Result: " + splitResult);
		LOG.info("********************");
		if ((int) splitResult[0] != -1) {
			//TODO load balance partitions among workers
			Random ran = new Random();
			MappingCache.updateMapping("" + (int)splitResult[1], StreamNetStaticComponents.slaves[ran.nextInt(
					StreamNetStaticComponents.slaves.length + 1)], ""+(int) splitResult[2], 
					StreamNetStaticComponents.slaves[ran.nextInt(StreamNetStaticComponents.slaves.length + 1)]);
			
			// create new partitions in Hive
			String[] addCommand = new String[] {"hive", "-e", "ALTER TABLE " + TABLE_NAME 
					+" ADD PARTITION (part = '" + (int) splitResult[1] + "') location 'part=" + (int) splitResult[1] + "';"};
			try {
				executeCommand(addCommand);
			} catch (IOException e) {
				System.err.println(e.toString());
			} catch (InterruptedException e) {
				System.err.println(e.toString());
			}
			addCommand = new String[] {"hive", "-e", "ALTER TABLE " + TABLE_NAME 
					+" ADD PARTITION (part = '" + (int) splitResult[2] + "') location 'part=" + (int) splitResult[2] + "';"};
			try {
				executeCommand(addCommand);
			} catch (IOException e) {
				System.err.println(e.toString());
			} catch (InterruptedException e) {
				System.err.println(e.toString());
			}
			
			// move data to new partitions
			Thread copyThread = new Thread(new CopyThread((int)splitResult[0], 
					(int)splitResult[1], (int) splitResult[2], splitResult[4], (int) splitResult[3], TABLE_NAME));
			copyThread.start();
			
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
							LOG.info("Everyone is up. Going to wait...");
							mutex.wait();
						}
						else {
							LOG.info("Some workers are down");
						}
					}
					else {
						LOG.info("Everyone is up. Going to wait...");
						mutex.wait();
					}
				} catch (KeeperException e) {
					
				} catch (InterruptedException e) {
					
				}
			}
		}
	}
}
