package gr.ntua.cslab.streamnet.zookeeper;

import gr.ntua.cslab.streamnet.beans.State;
import gr.ntua.cslab.streamnet.cache.KDtreeCache;
import gr.ntua.cslab.streamnet.cache.LeafPointsCache;
import gr.ntua.cslab.streamnet.cache.MappingCache;
import gr.ntua.cslab.streamnet.kdtree.KdTree;
import gr.ntua.cslab.streamnet.shared.StreamNetStaticComponents;
import gr.ntua.cslab.streamnet.threads.CopyThread;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
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

public class SyncWorker extends SyncPrimitive {
	public static boolean isAlive = false;
	private final String TABLE_NAME;
	private static final Logger LOG = Logger.getLogger(SyncWorker.class.getName());
	
	public SyncWorker (String address, int timeout, String root, String tableName, String boltName) {
        super(address, timeout, boltName);
        this.root = root;
        TABLE_NAME = tableName;

        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }
                s = zk.exists("/lock", false);
                if (s == null) {
                	zk.create("/lock", new byte[0], Ids.OPEN_ACL_UNSAFE,
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
	
	public boolean exists() {
		try {
			if (zk.getChildren(root, false).size() != 0 || zk.getChildren("/lock", false).size() != 0) 
					return true;
		} catch (KeeperException e) {
			LOG.info(e.toString());
		} catch (InterruptedException e) {
			LOG.info(e.toString());
		}
		return false;
	}
	
	public boolean writeState(HashMap<Integer, ArrayList<double[]>> newPoints) {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o;
		try {
			State st = new State(MappingCache.getFileMapping(), newPoints, KDtreeCache.getKd());
			o = new ObjectOutputStream(b);
			o.writeObject(st);
			byte[] state = b.toByteArray();
			/*b = new ByteArrayOutputStream();
			o = new ObjectOutputStream(b);
			o.writeObject(MappingCache.getFileMapping());
			byte[] mapping = b.toByteArray();
			byte[] points = null;
			
			if (newPoints != null) {
				b = new ByteArrayOutputStream();
				o = new ObjectOutputStream(b);
				o.writeObject(newPoints);
				points = b.toByteArray();
			}*/
			
			zk.create(root + "/state", state, Ids.OPEN_ACL_UNSAFE, 
        			CreateMode.PERSISTENT_SEQUENTIAL);
	        LOG.info("State written to Zookeeper");
	        /*zk.create(path + "/mapping", mapping, Ids.OPEN_ACL_UNSAFE, 
	    			CreateMode.PERSISTENT);
	        LOG.info("Mapping File written to Zookeeper");
	        zk.create(path + "/points", points, Ids.OPEN_ACL_UNSAFE,
	        		CreateMode.PERSISTENT);
	        LOG.info("Points File written to Zookeeper");
	        zk.create(path + "/kdtree", kdtree, Ids.OPEN_ACL_UNSAFE, 
        			CreateMode.PERSISTENT);
	        LOG.info("K-d Tree written to Zookeeper");*/
	        
	        return true;
		} catch (KeeperException e) {
			System.err.println( e.toString());
		} catch (InterruptedException e) {
			System.err.println(e.toString());
		} catch (IOException e) {
			System.err.println(e.toString());
		}
		return false;
	}
	
	private void releaseLock(String pathName) {
		LOG.info("Done with the lock. Releasing pathName: " + pathName);
		try {
			zk.delete(pathName, 0);
			LOG.info("Successfully released lock!!!");
		} catch (InterruptedException e) {
			LOG.info(e.toString());
		} catch (KeeperException e) {
			LOG.info(e.toString());
		}
	}
	
	public String readState() {
		Stat stat = null;
		
            synchronized (mutex) {
            	try {
                	if (!dead) {
                		String pathName = zk.create("/lock" + "/writelock", new byte[0], 
                				Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            			String myName = pathName.substring(15);
                		while (true) {
                			List<String> list = zk.getChildren("/lock", false);
                			Collections.sort(list);
                			int pos =0;
                			for(String s : list){
                    			LOG.info("Checking: " + s.substring(9)+" , "+myName);
                				if(s.substring(9).equals(myName)){
                					break;
                				}
                				pos++;
                			}
                			LOG.info("position: " + pos);
                			if (pos==0) {     
                				getState();
                				return pathName;
                			}
                			int prePos =pos-1;
                			LOG.info("Checking exists: /lock" + "/writeLock" +list.get(prePos).substring(9));
                			stat = zk.exists("/lock" + "/writeLock" +list.get(prePos).substring(9), true);
                			if (stat == null) 
                				continue;
                			else {
                				LOG.info("Going to wait because someone else is trying to write");
                				mutex.wait();
                			}
                		}
                	} else {
                		return null;
                	}
            	} catch (KeeperException e) {
            		System.err.println("Keeper exception when trying to read data "
            				+ "from Zookeeper: " + e.toString());
            	} catch (InterruptedException e) {
            		System.err.println("Interrupted exception when trying to read data "
            				+ "from Zookeeper: " + e.toString());
            	}
            }
//         should never reach this point
			return null;
	}
	
	public boolean update(int id) {
		// update KDtreeCache, MappingCache, LeafPointsCache
		String pathName = readState();
		LOG.info("Before K-d Tree is split! Split node is: " + id);
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
			LOG.info("Going to split node now!");
			//TODO load balance partitions among workers
			Random ran = new Random();
			int worker1 = ran.nextInt(8) + 1;
			int worker2 = ran.nextInt(8) + 1;
			while (worker2 == worker1) {
				worker2 = ran.nextInt(8) + 1;
			}
			MappingCache.updateMapping("" + (int)splitResult[1], "worker" + worker1, ""+(int) splitResult[2], "worker" + worker2);
			
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
			/*Thread copyThread = new Thread(new CopyThread((int)splitResult[0], 
					(int)splitResult[1], (int) splitResult[2], splitResult[3], (int) splitResult[4], TABLE_NAME));
			copyThread.start();*/
			
			Path pt = new Path("hdfs://master:9000/opt/warehouse/" + TABLE_NAME 
					+ "/part=" + oldId + "/part-" + oldId + ".gz");
//			while (true) {
				try {
					FileSystem fs = FileSystem.get(new Configuration());
					BufferedReader br = new BufferedReader(new BufferedReader(
							new InputStreamReader(new GZIPInputStream(fs.open(pt)), "UTF-8")));
					Path ptLeft = new Path("hdfs://master:9000/opt/warehouse/" + TABLE_NAME 
							+ "/part=" + leftId + "/part-" + leftId + ".gz");
					BufferedWriter bwLeft = new BufferedWriter(new OutputStreamWriter(
							new GZIPOutputStream(fs.create(ptLeft))));
					Path ptRight = new Path("hdfs://master:9000/opt/warehouse/" + TABLE_NAME 
							+ "/part=" + rightId + "/part-" + rightId + ".gz");
					BufferedWriter bwRight = new BufferedWriter(new OutputStreamWriter(
							new GZIPOutputStream(fs.create(ptRight))));
					String line;
					while ((line = br.readLine()) != null) {
						String[] parts1 = line.split(" ");
						double value = 0;
				
						switch (splitDimension) {
						case 0:	value = Double.parseDouble(parts1[1]);
							break;
						case 1:	value = Double.parseDouble(parts1[3]);
						case 2:	SimpleDateFormat formatter = 
											new SimpleDateFormat("yyyy-MM-dd");
					 					try {
					 							Date dateStr = formatter.parse(parts1[8]);
					 							value = (double) dateStr.getTime();
					 					} catch (ParseException e) {
					 							e.printStackTrace();
					 					}
					 					break;
	                     
						default: System.err.println("Dimension number " + splitDimension);
	            		 				break;
						}
						if (value > splitValue) {
							bwRight.write(line);
							bwRight.newLine();
						}
						else {
							bwLeft.write(line);
							bwLeft.newLine();
						}
					}
					br.close();
					bwLeft.close();
					bwRight.close();
//					break;
				} catch (IOException e) {
					System.err.println(e.toString());
				}
//			}
			
			String[] delCommand = new String[] {"hive", "-e", "ALTER TABLE " + TABLE_NAME 
					+" DROP PARTITION (part = '" + oldId + "');"};
			try {
				executeCommand(delCommand);
			} catch (IOException e) {
				System.err.println(e.toString());
			} catch (InterruptedException e) {
				System.err.println(e.toString());
			}
			
			// update zookeeper views
			this.writeState(newPoints);
			
//			release lock so others can write
			releaseLock(pathName);
			
			return true;
		}
		else {
			LOG.info("Something went wrong. Not performing split!!!");
			return false;
		}
	}
}
