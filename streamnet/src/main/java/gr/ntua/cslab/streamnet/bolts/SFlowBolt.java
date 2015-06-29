package gr.ntua.cslab.streamnet.bolts;

import gr.ntua.cslab.streamnet.beans.PartitionInfo;
import gr.ntua.cslab.streamnet.beans.SflowsList;
import gr.ntua.cslab.streamnet.cache.KDtreeCache;
import gr.ntua.cslab.streamnet.cache.LeafPointsCache;
import gr.ntua.cslab.streamnet.cache.MappingCache;
import gr.ntua.cslab.streamnet.cache.SFlowsCache;
import gr.ntua.cslab.streamnet.kdtree.KdTree;
import gr.ntua.cslab.streamnet.shared.StreamNetStaticComponents;
import gr.ntua.cslab.streamnet.threads.ZkReadThread;
import gr.ntua.cslab.streamnet.zookeeper.SyncWorker;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SFlowBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(SFlowBolt.class.getName());
	OutputCollector _collector;
	TopologyContext _topo;
	private String boltName;
	private final String TABLE_NAME;
	
	public SFlowBolt(String boltName) {
		this.boltName = boltName;
		this.TABLE_NAME = StreamNetStaticComponents.TABLE_NAME;
	}
	
	private PartitionInfo getPartitionNumber(String record) {
		String[] parts = record.split(" ");
		double[] point = new double[KDtreeCache.getDimensions().length];
		
		for (int i = 0; i < KDtreeCache.getDimensions().length; i++) {
			switch (Integer.parseInt(KDtreeCache.getDimensions()[i])) {
        		case 1:  point[i] = Double.parseDouble(parts[1]);
        			break;
        		case 3:  point[i] = Double.parseDouble(parts[3]);
        			break;
        		case 8:  SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        			try {
        				Date dateStr = formatter.parse(parts[8]);
        				point[i] = (double) dateStr.getTime();
        			} catch (ParseException e) {
        				LOG.info("Failed: Parse Error in Date format string. Accepted format yyyy-MM-dd");
        				e.printStackTrace();
        			}
        			break;
        		default: LOG.info("Dimension number n");
        			break;
			}
		}
		
		return new PartitionInfo(KDtreeCache.getKd().find(point), point);
	}
	
	@Override
	public void execute(Tuple record)  {
		LOG.info("Processing record...");
		String sFlowRecord = record.getString(0);
		String[] parts = sFlowRecord.split(" ");
		String[] ipFrom = parts[0].split("\\.");
		String[] ipTo = parts[2].split("\\.");
		
		if (ipFrom.length > 1 && ipTo.length > 1) {
			PartitionInfo pInfo = getPartitionNumber(sFlowRecord);
			int partitionId = pInfo.getPartitionId();
			double[] point = pInfo.getPoint();
			
			String worker = MappingCache.getFileMapping().get("" + partitionId);
			
			List<Integer> myList = _topo.getComponentTasks(boltName);
			LOG.info("List of IDs: " + myList.toString());
			// if record belongs to another worker send it there
			if ( !worker.equals(boltName) || myList.get(partitionId % myList.size()) != _topo.getThisTaskId() ) {
				List<Integer> l = _topo.getComponentTasks(worker);
				// emit direct to the correct worker
				LOG.info("Worker name: " + boltName + " id: " + _topo.getThisTaskId());
				LOG.info("Sending record to appropriate worker");
				_collector.emitDirect(l.get(partitionId % l.size()), new Values(sFlowRecord));
			}
			else {
				LOG.info("Worker name: " + boltName + " id: " + _topo.getThisTaskId() +
						" pos in list: " + myList.get(partitionId % myList.size()));
				Random ran = new Random();
				if (ran.nextDouble() < 0.05) {
					LOG.info("Added Point in Kd-Tree");
					KDtreeCache.getKd().addPoint(partitionId);
					LeafPointsCache.addPoint(partitionId, point);
				}
				SFlowsCache.updateSflowsToStore(partitionId, sFlowRecord);
				LOG.info("Added Point in cache");
				
				// check if SFlowsCache is full
				// if so, write data to HDFS
				ArrayList<Integer> keysRemoved = new ArrayList<Integer>();
				Iterator<Integer> keySet = SFlowsCache.getSflowsToStore().keySet().iterator();
				if (SFlowsCache.fullSflowsToStore()) {
					while (keySet.hasNext()) {
						int key = keySet.next();
						if (KDtreeCache.getKd().isLeaf(key)) {
							Configuration conf = new Configuration();
							conf.set("fs.hdfs.impl", 
							        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
							 conf.set("fs.file.impl",
							        org.apache.hadoop.fs.LocalFileSystem.class.getName());
							FileSystem fs = null;
							long length = 0;
							try {
								fs = FileSystem.get(conf);
								length = fs.getFileStatus(new Path("hdfs://master:9000/opt/warehouse/" 
										+ TABLE_NAME + "/part=" + key + "/part-" + key + ".gz")).getLen();
							} catch (IOException e1) {
								LOG.info(e1.getMessage());
							}
							if (length < 500000) {
								// file is below block size, so just write data to it
								while (true) {
									try {
										SflowsList sflowsList = SFlowsCache.getSflowsToStore().get(key);
										//use key to open the correct file
										Path pt = new Path("hdfs://master:9000/opt/warehouse/" 
												+ TABLE_NAME + "/part=" + key + "/part-" + key + ".gz");
										BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
												new GZIPOutputStream(fs.append(pt)), "UTF-8"));
										for (String r : sflowsList.getSflowsList()) {
											bw.write(r);
											bw.newLine();
										}
										bw.close();
										LOG.info("Successfully written data to HDFS file");
										//clean up SflowsToStore HashMap
										keysRemoved.add(key);
										break;
									} catch (IOException e) {
										LOG.info(e.getMessage());
									}		
								}
							}
							else {
								// file exceeds block size, so we have to perform a split
								SyncWorker sw = new SyncWorker("master:2181", 2000000, "/datix", "/lock", TABLE_NAME, boltName);
								LOG.info("Performing a split in Kd-Tree");
								sw.update(key);
//								Thread splitThread = new Thread(new SplitThread("master:2181", "/datix", key, TABLE_NAME));
//								splitThread.start();
							}
						}
						else {
							for (String sFlowRecord1 : SFlowsCache.getSflowsToStore().get(key).getSflowsList()) {
								pInfo = getPartitionNumber(sFlowRecord1);
								partitionId = pInfo.getPartitionId();
								String worker1 = MappingCache.getFileMapping().get("" + partitionId);
								List<Integer> l = _topo.getComponentTasks(worker1);
								// emit direct to the correct worker
								LOG.info("Sending record to appropriate worker");
								_collector.emitDirect(l.get(partitionId % l.size()), new Values(sFlowRecord1));
								 keysRemoved.add(key);
							}
						}
					}
				}
				SFlowsCache.cleanSflowToStore(keysRemoved);
			}
		}
		_collector.ack(record);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext topo, 
			OutputCollector collector) {
		_collector = collector;
		_topo = topo;
		int waitTime = 10000;
		// initialize memory caches
		SyncWorker sw = new SyncWorker("master:2181", 2000000, "/datix", 
				"/lock", TABLE_NAME, boltName);
		if (!sw.exists()) {
			KDtreeCache.setKd(new KdTree<Long>(KDtreeCache.getDimensions().length,
					KDtreeCache.getBucketSize()));
			MappingCache.setFileMapping(new HashMap <String, String>());
			MappingCache.updateMapping("1", "worker1");
			LeafPointsCache.setPoints(new HashMap<Integer, ArrayList<double[]>>());
			if (boltName.equals("worker1")) {
				if (_topo.getThisTaskId() == _topo.getComponentTasks(boltName).get(0)) {
					sw.writeState(null);
				}
			}
		}
		else {
			sw.getState();
		}
		SFlowsCache.setSflowsToStore(new HashMap<Integer, SflowsList>());
		Thread zkReadThread = new Thread(new ZkReadThread("master:2181",
				"/datix", "/lock", TABLE_NAME, boltName, waitTime));
		zkReadThread.start();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("record"));
	}
}
