package gr.ntua.cslab.streamnet.zookeeper;

import gr.ntua.cslab.streamnet.beans.State;
import gr.ntua.cslab.streamnet.cache.KDtreeCache;
import gr.ntua.cslab.streamnet.cache.LeafPointsCache;
import gr.ntua.cslab.streamnet.cache.MappingCache;
import gr.ntua.cslab.streamnet.shared.StreamNetStaticComponents;
import gr.ntua.cslab.streamnet.threads.CopyThread;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import backtype.storm.task.TopologyContext;

public class SyncWorker extends SyncPrimitive {
	public static boolean isAlive = false;
	private final String TABLE_NAME;
	private int boltNo;
	private static final Logger LOG = Logger.getLogger(SyncWorker.class.getName());
	
	public SyncWorker (String address, int timeout, String stateRoot, String lockRoot, 
			String tableName, String boltName, TopologyContext topo, int boltNo) {
        super(address, timeout, boltName, topo);
        this.stateRoot = stateRoot;
        this.lockRoot = lockRoot;
        this.boltNo = boltNo;
        TABLE_NAME = tableName;

        if (zk != null) {
        	while (true) {
        		try {
        			Stat s = zk.exists(stateRoot, false);
        			if (s == null) {
        				zk.create(stateRoot, new byte[0], Ids.OPEN_ACL_UNSAFE,
        						CreateMode.PERSISTENT);
        			}
        			s = zk.exists(lockRoot, false);
        			if (s == null) {
        				zk.create(lockRoot, new byte[0], Ids.OPEN_ACL_UNSAFE,
        						CreateMode.PERSISTENT);
        			}
        			return;
        		} catch (KeeperException e) {
//            		restartZK();
        			LOG.info("Keeper exception when instantiating Datix: "
                                	+ e.toString());
        			e.printStackTrace();
        		} catch (InterruptedException e) {
//            		restartZK();
        			LOG.info("Interrupted exception");
        			e.printStackTrace();
        		}
        	}
        }
	}
	
	private Map<String,String> executeCommand(String[] command) throws IOException, InterruptedException {
		String c="Executing command: ";
		for (int i = 0; i < command.length; i++) {
			c+=command[i]+" ";
		}
//		LOG.info(c);
		
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
//        LOG.info("Command Output: "+output.toString());
        ret.put("output", output.toString());
		reader = new BufferedReader(new InputStreamReader(p1.getErrorStream()));
		line = "";	
		output = new StringBuffer();		
		while ((line = reader.readLine())!= null) {
			output.append(line + "\n");
		}
//        LOG.info("Command Error: "+output.toString());
        ret.put("error", output.toString());
		return ret; 
	}
	
	public boolean exists() {
		while (true) {
			try {
				if (zk.getChildren(stateRoot, false).size() != 0 || zk.getChildren(lockRoot, false).size() != 0) 
						return true;
				else return false;
			} catch (KeeperException e) {
//        		restartZK();
				LOG.info(e.toString());
				e.printStackTrace();
			} catch (InterruptedException e) {
//        		restartZK();
				LOG.info(e.toString());
				e.printStackTrace();
			}
		}
//		return false;
	}
	
	public boolean writeState(HashMap<Integer, ArrayList<double[]>> newPoints) {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o;
        while (true) {
        	try {
        		State st = new State(MappingCache.getFileMapping(), newPoints, KDtreeCache.getKd());
        		o = new ObjectOutputStream(b);
        		o.writeObject(st);
        		byte[] state = b.toByteArray();
			
        		zk.create(stateRoot + "/state", state, Ids.OPEN_ACL_UNSAFE, 
        				CreateMode.PERSISTENT_SEQUENTIAL);
//	            LOG.info("State written to Zookeeper");
	        
        		return true;
        	} catch (KeeperException e) {
//        		restartZK();
        		LOG.info("*****" + e.toString());
        		e.printStackTrace();
        	} catch (InterruptedException e) {
//        		restartZK();
        		LOG.info("*****" + e.toString());
        		e.printStackTrace();
        	} catch (IOException e) {
//        		restartZK();
        		LOG.info("*****");
        		System.err.println(e.toString());
        		e.printStackTrace();
        	}
        }
//		return false;
	}
	
	private void releaseLock(String pathName) {
//		LOG.info("Done with the lock. Releasing pathName: " + pathName);
		while (true) {
			try {
				zk.delete(pathName, 0);
				return;
//			LOG.info("Successfully released lock!!!");
			} catch (InterruptedException e) {
//        		restartZK();
				LOG.info(e.toString());
				e.printStackTrace();
			} catch (KeeperException e) {
//        		restartZK();
				LOG.info(e.toString());
				e.printStackTrace();
			}
		}
	}
	
	public String acquireLock() {
		Stat stat = null;
		
            synchronized (mutex) {
            	while (true) {
            		try {
            			if (!dead) {
            				String pathName = zk.create(lockRoot + "/writelock", new byte[0], 
            						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            				String myName = pathName.substring(15);
            				while (true) {
            					List<String> list = zk.getChildren(lockRoot, false);
            					Collections.sort(list);
            					int pos =0;
            					for(String s : list){
//                    				LOG.info("Checking: " + s.substring(9)+" , "+myName);
            						if(s.substring(9).equals(myName)){
            							break;
            						}
            						pos++;
            					}
//                				LOG.info("position: " + pos);
            					if (pos==0) {     
            						getState();
                					return pathName;
            					}
            					int prePos =pos-1;
//                				LOG.info("Checking exists: " + lockRoot + "/writeLock" +list.get(prePos).substring(9));
            					stat = zk.exists(lockRoot + "/writeLock" +list.get(prePos).substring(9), true);
            					if (stat == null) 
            						continue;
            					else {
//                					LOG.info("Going to wait because someone else is trying to write");
            						mutex.wait();
            					}
            				}
            			} else {
            				return null;
            			}
            		} catch (KeeperException e) {
//                		restartZK();
            			LOG.info("!!!Keeper exception when trying to read data "
            					+ "from Zookeeper: " + e.toString());
            			e.printStackTrace();
            		} catch (InterruptedException e) {
//                		restartZK();
            			LOG.info("!!!Interrupted exception when trying to read data "
            					+ "from Zookeeper: " + e.toString());
            			e.printStackTrace();
            		}
            	}
            }
//         should never reach this point
//			return null;
	}
	
	public void blockingStateRead() {	
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
						e.printStackTrace();
					}*/
                } else {
                	return;
                }
            }
        }
	}
	
	public boolean update(int id) {
		// update KDtreeCache, MappingCache, LeafPointsCache
		String pathName = acquireLock();
//		LOG.info("Keyset in LeafPointsCache: " + LeafPointsCache.getPoints().keySet());
//		LOG.info("Before K-d Tree is going to be split! Split node is: " + id);
		double[] splitResult = KDtreeCache.getKd().performSplit(id);
		int oldId = (int) splitResult[0];
		int leftId = (int) splitResult[1];
		int rightId = (int) splitResult[2];
		int splitDimension = (int) splitResult[4];
		double splitValue = splitResult[3];
		
		// Move points into children
		HashMap<Integer, ArrayList<double[]>> newPoints = new HashMap<Integer, ArrayList<double[]>>();
		LeafPointsCache.splitPoints(oldId, leftId, rightId, splitDimension, splitValue);
		newPoints.put(leftId, LeafPointsCache.getPoints().get(leftId));
		newPoints.put(rightId, LeafPointsCache.getPoints().get(rightId));
		LeafPointsCache.deletePoints(leftId);
		LeafPointsCache.deletePoints(rightId);
		LeafPointsCache.deletePoints(oldId);
		
		if ((int) splitResult[0] != -1) {
//			LOG.info("Going to split node now!");
			//TODO load balance partitions among workers
			List<Integer> workers = topo.getComponentTasks("worker");
			Random ran = new Random();
			int worker1 = ran.nextInt(workers.size());
			int worker2 = ran.nextInt(workers.size());
			while (worker2 == worker1) {
				worker2 = ran.nextInt(workers.size());
			}
			MappingCache.updateMapping("" + (int)splitResult[1], "" + workers.get(worker1), ""+(int) splitResult[2], "" + workers.get(worker2));
			
			// create new partitions in Hive
			String[] addCommand = new String[] {"hive", "-e", "ALTER TABLE " + TABLE_NAME 
					+" ADD PARTITION (part = '" + (int) splitResult[1] + "') location 'part=" + (int) splitResult[1] + "';"};
			try {
				executeCommand(addCommand);
			} catch (IOException e) {
				LOG.info("!!!" + e.toString());
				e.printStackTrace();
			} catch (InterruptedException e) {
				LOG.info("!!!" + e.toString());
				e.printStackTrace();
			}
			addCommand = new String[] {"hive", "-e", "ALTER TABLE " + TABLE_NAME 
					+" ADD PARTITION (part = '" + (int) splitResult[2] + "') location 'part=" + (int) splitResult[2] + "';"};
			try {
				executeCommand(addCommand);
			} catch (IOException e) {
				LOG.info(e.toString());
				e.printStackTrace();
			} catch (InterruptedException e) {
				LOG.info(e.toString());
				e.printStackTrace();
			}
			
			// move data to new partitions
			Thread copyThread = new Thread(new CopyThread((int)splitResult[0], 
					(int)splitResult[1], (int) splitResult[2], splitResult[3], (int) splitResult[4], TABLE_NAME));
			copyThread.start();
			
			// update zookeeper views
			this.writeState(newPoints);
			
//			release lock so others can write
			releaseLock(pathName);
			
			return true;
		}
		else {
//			LOG.info("Something went wrong. Not performing split!!!");
			return false;
		}
	}
}
