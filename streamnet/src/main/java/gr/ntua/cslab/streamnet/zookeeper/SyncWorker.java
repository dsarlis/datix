package gr.ntua.cslab.streamnet.zookeeper;

import gr.ntua.cslab.streamnet.cache.KDtreeCache;
import gr.ntua.cslab.streamnet.cache.LeafPointsCache;
import gr.ntua.cslab.streamnet.cache.MappingCache;
import gr.ntua.cslab.streamnet.kdtree.KdTree;
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
import java.util.ArrayList;
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
	private final String BOLT_NAME;
	private static final Logger LOG = Logger.getLogger(SyncWorker.class.getName());
	
	public SyncWorker (String address, int timeout, String root, String tableName, String boltName) {
        super(address, timeout);
        this.root = root;
        TABLE_NAME = tableName;
        BOLT_NAME = boltName;

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
	
	public boolean write(HashMap<Integer, ArrayList<double[]>> newPoints) {
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
			byte[] points = null;
			
			if (newPoints != null) {
				b = new ByteArrayOutputStream();
				o = new ObjectOutputStream(b);
				o.writeObject(newPoints);
				points = b.toByteArray();
			}
				
			Stat s = null;
			s = zk.exists(root + "/kdtree", false);
			if (s != null) {
	        	zk.delete(root + "/kdtree", 0);
//	        	LOG.info("Deleted K-d Tree from Zookeeper");
	        }
	        s = zk.exists(root + "/mapping", false);
	        if (s != null) {
	        	zk.delete(root + "/mapping", 0);
//	        	LOG.info("Deleted Mapping file from Zookeeper");
	        }
	        s = zk.exists(root + "/points", false);
	        if (s != null) {
	        	zk.delete(root + "/points", 0);
//	        	LOG.info("Deleted Points file from Zookeeper");
	        }
	        
	        zk.create(root + "/mapping", mapping, Ids.OPEN_ACL_UNSAFE, 
	    			CreateMode.PERSISTENT);
	        LOG.info("Mapping File written to Zookeeper");
	        zk.create(root + "/points", points, Ids.OPEN_ACL_UNSAFE,
	        		CreateMode.PERSISTENT);
	        LOG.info("Points File written to Zookeeper");
	        zk.create(root + "/kdtree", kdtree, Ids.OPEN_ACL_UNSAFE, 
        			CreateMode.PERSISTENT);
	        LOG.info("K-d Tree written to Zookeeper");
	        zk.create("/lock", new byte[0], Ids.OPEN_ACL_UNSAFE,
	        		CreateMode.PERSISTENT);
	        LOG.info("Lock File written to Zookeeper");
	        
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
	
	@SuppressWarnings("unchecked")
	public void read() {
		Stat stat = null;
		
		while (true) {
            synchronized (mutex) {
            	try {
//            		List<String> list = zk.getChildren(root, true);
                	if (!dead) {
                		if (zk.exists("/lock", true) != null) {
                			zk.delete("/lock", 0);
                			byte[] b = zk.getData(root + "/mapping", false, stat);
                			ObjectInputStream o = new ObjectInputStream(new ByteArrayInputStream(b));
                			LOG.info("Mapping File read from Zookeeper");
                			HashMap<String, String> newMapping = (HashMap<String, String>) o.readObject();
                			BufferedWriter bw = new BufferedWriter(new 
                					FileWriter("/tmp/mapping_dup_from_zookeeper"));
                			for (String key: MappingCache.getFileMapping().keySet()) {
                				bw.write("key: " + key + " value: " + newMapping.get(key));
                				bw.newLine();
                			}
                			o.close();
                			bw.close();
                			if (newMapping.keySet().size() > MappingCache.getFileMapping().keySet().size())
                				MappingCache.setFileMapping(newMapping);
                			bw = new BufferedWriter(new FileWriter("/tmp/mapping_dup"));
                			for (String key: MappingCache.getFileMapping().keySet()) {
                				bw.write("key: " + key + " value: " + MappingCache.getFileMapping().get(key));
                				bw.newLine();
                			}
                			o.close();
                			bw.close();
                			
                			b = zk.getData(root + "/points", false, stat);
                			LOG.info("Points File read from Zookeeper");
                			if (b != null) {
                				o = new ObjectInputStream(new ByteArrayInputStream(b));
                				HashMap<Integer, ArrayList<double[]>> newPoints = 
                						(HashMap<Integer, ArrayList<double[]>>) o.readObject();
                				for (int key: newPoints.keySet()) {
                					String worker = MappingCache.getFileMapping().get(""+key);
                					/*bw = new BufferedWriter(new FileWriter("/tmp/leaf_points_from_zookeeper"));
                					for (double[] point: newPoints.get(key)) {
                						bw.write("point: " + point[0] + "," + point[1] + "," + point[2]);
                        				bw.newLine();
                        			}
                					bw.close();*/
                					if (worker.equals(BOLT_NAME)) {
                						for (double[] point: newPoints.get(key)) {
                							LeafPointsCache.addPoint(key, point);
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
                    			o.close();
                    			bw.close();
                			}
                			
                			b = zk.getData(root + "/kdtree",
            							false, stat);
                			LOG.info("K-d Tree read from Zookeeper");
                			o = new ObjectInputStream(
                					new ByteArrayInputStream(b));
                			KdTree<Long> newKd = (KdTree<Long>) o.readObject();
                			if (newKd.countLeafs() > KDtreeCache.getKd().countLeafs())
                				KDtreeCache.setKd(newKd);
                			bw = new BufferedWriter(new FileWriter("/tmp/kdtree_dup"));
                			KDtreeCache.getKd().printTree(bw);
                			o.close();
                			bw.close();
                			break;
                		}
                		else {
                			LOG.info("Going to wait because someone else is trying to write");
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
	
	@SuppressWarnings("unchecked")
	public void blockingRead() {
		Stat stat = null;
		
		while (true) {
            synchronized (mutex) {
            	try {
            		List<String> list = zk.getChildren(root, true);
                	if (!dead) {
                		if (list.size() == 3) {
                			byte[] b = zk.getData(root + "/mapping", false, stat);
                			LOG.info("Mapping File read from Zookeeper");
                			ObjectInputStream o = new ObjectInputStream(new ByteArrayInputStream(b));
                			HashMap<String, String> newMapping = (HashMap<String, String>) o.readObject();
                			if (newMapping.keySet().size() > MappingCache.getFileMapping().keySet().size())
                				MappingCache.setFileMapping(newMapping);
                			BufferedWriter bw = new BufferedWriter(new FileWriter("/tmp/mapping_dup"));
                			for (String key: MappingCache.getFileMapping().keySet()) {
                				bw.write("key: " + key + " value: " + MappingCache.getFileMapping().get(key));
                				bw.newLine();
                			}
                			o.close();
                			bw.close();
                			
                			b = zk.getData(root + "/points", false, stat);
                			LOG.info("Points File read from Zookeeper");
                			if (b != null) {
                				o = new ObjectInputStream(new ByteArrayInputStream(b));
                				HashMap<Integer, ArrayList<double[]>> newPoints = 
                						(HashMap<Integer, ArrayList<double[]>>) o.readObject();
                				for (int key: newPoints.keySet()) {
                					String worker = MappingCache.getFileMapping().get(""+key);
                					/*bw = new BufferedWriter(new FileWriter("/tmp/leaf_points_from_zookeeper"));
                					for (double[] point: newPoints.get(key)) {
                        				bw.write("point: " + point[0] + "," + point[1] + "," + point[2]);
                        				bw.newLine();
                        			}
                					bw.close();*/
                					if (worker.equals(BOLT_NAME)) {
                						for (double[] point: newPoints.get(key)) {
                							LeafPointsCache.addPoint(key, point);
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
                    			o.close();
                    			bw.close();
                			}
                			
                			b = zk.getData(root + "/kdtree",
            							false, stat);
                			LOG.info("K-d Tree read from Zookeeper");
                			o = new ObjectInputStream(new ByteArrayInputStream(b));
                			KdTree<Long> newKd = (KdTree<Long>) o.readObject();
                			if (newKd.countLeafs() > KDtreeCache.getKd().countLeafs())
                				KDtreeCache.setKd(newKd);
                			bw = new BufferedWriter(new FileWriter("/tmp/kdtree_dup"));
                			KDtreeCache.getKd().printTree(bw);
                			o.close();
                			bw.close();
                		}
                		LOG.info("Going to wait for poll-reading");
                		mutex.wait();
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
		read();
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
		
		if ((int) splitResult[0] != -1) {
			LOG.info("Going to split node now!");
			//TODO load balance partitions among workers
			Random ran = new Random();
			int worker1 = ran.nextInt(5) + 1;
			int worker2 = ran.nextInt(5) + 1;
			while (worker2 == worker1) {
				worker2 = ran.nextInt(5) + 1;
			}
			MappingCache.updateMapping("" + (int)splitResult[1], "worker" + worker1, ""+(int) splitResult[2], "worker" + worker2);
			/*for (String key: MappingCache.getFileMapping().keySet()) {
				LOG.info("key: " + key + " worker: " + MappingCache.getFileMapping().get(key));
			}*/
			
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
					(int)splitResult[1], (int) splitResult[2], splitResult[3], (int) splitResult[4], TABLE_NAME));
			copyThread.start();
			
			/*Path pt = new Path("hdfs://master:9000/opt/warehouse/" + TABLE_NAME 
					+ "/part=" + oldId + "/part-" + oldId + ".gz");
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
	        System.out.println("------->Splitvalue chosen: " + splitValue + " SplitDimension: " + splitDimension);
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
			} catch (IOException e) {
				System.err.println(e.toString());
			}*/
			
			// update zookeeper views
			this.write(newPoints);
//			System.out.println("-------->Written new data structures in Zookeeper");
			
			return true;
		}
		else {
			LOG.info("Something went wrong. Not performing split!!!");
			return false;
		}
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
