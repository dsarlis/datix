package gr.ntua.cslab.streamnet.bolts;

import gr.ntua.cslab.streamnet.beans.PartitionInfo;
import gr.ntua.cslab.streamnet.beans.SflowsList;
import gr.ntua.cslab.streamnet.cache.KDtreeCache;
import gr.ntua.cslab.streamnet.cache.LeafPointsCache;
import gr.ntua.cslab.streamnet.cache.MappingCache;
import gr.ntua.cslab.streamnet.cache.SFlowsCache;
import gr.ntua.cslab.streamnet.threads.SplitThread;
import gr.ntua.cslab.streamnet.threads.ZkReadThread;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SFlowBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(SFlowBolt.class);
	OutputCollector _collector;
	TopologyContext _topo;
	private String boltName;
	private final String TABLE_NAME;
	
	public SFlowBolt(String boltName, String tableName) {
		this.boltName = boltName;
		this.TABLE_NAME = tableName;
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
        				LOG.error("Failed: Parse Error in Date format string. Accepted format yyyy-MM-dd");
        				e.printStackTrace();
        			}
        			break;
        		default: LOG.error("Dimension number n");
        			break;
			}
		}
		
		return new PartitionInfo(KDtreeCache.getKd().find(point), point);
	}
	
	@Override
	public void execute(Tuple record) {
		String sFlowRecord = record.getString(record.fieldIndex("record"));
		String[] parts = sFlowRecord.split(" ");
		String[] ipFrom = parts[0].split("\\.");
		String[] ipTo = parts[2].split("\\.");
		
		if (ipFrom.length > 1 && ipTo.length > 1) {
			PartitionInfo pInfo = getPartitionNumber(sFlowRecord);
			int partitionId = pInfo.getPartitionId();
			double[] point = pInfo.getPoint();
			
			String worker = MappingCache.getFileMapping().get("" + partitionId);
			
			// if record belongs to another worker send it there
			if (!worker.equals(boltName)) {
				List<Integer> l = _topo.getComponentTasks(worker);
				// emit direct to the correct worker
				LOG.info("Sending record to appropriate worker");
				// TODO if multiple threads for the same worker
				_collector.emitDirect(l.get(0), new Values(record));
			}
			else {
				LeafPointsCache.addPoint(partitionId, point);
				SFlowsCache.updateSflowsToStore(partitionId, sFlowRecord);
				LOG.info("Added Point in cache");
				
				// check if SFlowsCache is full
				// if so, write data to HDFS
				if (SFlowsCache.fullSflowsToStore()) {
					for (int key : SFlowsCache.getSflowsToStore().keySet()) {
						
						if (KDtreeCache.getKd().isLeaf(key)) {
							Configuration conf = new Configuration();
							FileSystem fs = null;
							long length = 0;
							try {
								fs = FileSystem.get(conf);
								length = fs.getFileStatus(new Path("hdfs://master:9000/opt/warehouse/" 
										+ TABLE_NAME + "/part=" + key + "/part-" + key + ".gz")).getLen();
							} catch (IOException e1) {
								LOG.error("Failed to get HDFS Configuration");
							}
							if (length < 67108864) {
								// file is below block size, so just write data to it
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
									SFlowsCache.deleteKeyFromSflowsToStore(key);
								} catch (IOException e) {
									LOG.error("Failed: Not able to append to HDFS file!");
								}		
							}
							else {
								// file exceeds block size, so we have to perform a split
								Thread splitThread = new Thread(new SplitThread("master:2181", "/datix", key, TABLE_NAME));
								splitThread.start();
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
								// TODO if multiple threads for the same worker
								_collector.emitDirect(l.get(0), new Values(record));
							}
						}
					}
				}
			}
		}
	}

	@Override
	public void prepare(Map conf, TopologyContext topo, OutputCollector collector) {
		_collector = collector;
		_topo = topo;
		Thread zkReadThread = new Thread(new ZkReadThread("master:2181", "/datix", TABLE_NAME));
		zkReadThread.start();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("record"));
	}
}
