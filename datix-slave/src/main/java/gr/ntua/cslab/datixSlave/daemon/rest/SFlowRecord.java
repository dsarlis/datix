package gr.ntua.cslab.datixSlave.daemon.rest;

import gr.ntua.cslab.datixSlave.beans.SflowsList;
import gr.ntua.cslab.datixSlave.beans.SflowsToStoreList;
import gr.ntua.cslab.datixSlave.daemon.cache.KDtreeCache;
import gr.ntua.cslab.datixSlave.daemon.cache.MappingCache;
import gr.ntua.cslab.datixSlave.daemon.cache.SFlowsCache;
import gr.ntua.cslab.datixSlave.daemon.shared.SlaveStaticComponents;
import gr.ntua.cslab.datixSlave.daemon.threads.StoreSflows;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;


@Path("/sflows/")
public class SFlowRecord {
	
	public Logger logger = Logger.getLogger(SFlowRecord.class);
	private static Lock myLock = new ReentrantLock();
	
	@Path("dispatch/")
	@POST
	@Consumes(MediaType.APPLICATION_XML)
	public synchronized String dispatchSFlowRecord(String input) throws FileNotFoundException {
		
//		synchronized(SlaveStaticComponents.getLock()) {
		try {
			SlaveStaticComponents.getLock().lock();
			//create point from appropriate SFlow record fields
			logger.info("Input: " + input);
			input = input.substring(0, input.length()-1);
			String[] records = input.split(",");
			
			for (String record : records) {
			
				String[] parts = record.split(" ");
				String[] ipFrom = parts[0].split("\\.");
				String[] ipTo = parts[2].split("\\.");
		 
				if (ipFrom.length > 1 && ipTo.length > 1) {
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
		            				logger.error("Failed: Parse Error in Date format string. Accepted format yyyy-MM-dd");
		            				e.printStackTrace();
		            			}
		            			break;
		            		default: System.err.println("Dimension number n");
		            			break;
						}
					}
			 
					//find partition_no
					int partition_id = KDtreeCache.getKd().find(point);

					//add new record to hashmap
					logger.info("Before inserting");
					SFlowsCache.updateCachedSflows(""+partition_id, record);
					logger.info("Record: " + record + " added in HashMap");
				}
//				return "Sflow record rejected! Not an IPv4 packet\n";
			}
			
			//if hashmap is full?? dispatch records to appropriate slaves
			if (SFlowsCache.fullCachedSflows()) {
				
				Set<String> keySet = SFlowsCache.getCachedSflows().keySet();
				Client client = Client.create();
				for (String key : keySet) {
					 	
					String slave_id = MappingCache.getFileMapping().get(""+key);
					if (slave_id == null) {
						logger.fatal("Failed: No node assignment for the particular partition.");
					}
					WebResource webResource = client.resource("http://" + slave_id + ":8085/sflows/store");
					  	
//					WebResource webResource = client.resource("http://localhost:8085/sflows/store");
					SFlowsCache.getCachedSflows().get(key).updateList(key);
					ClientResponse response = webResource.type("application/xml").post(ClientResponse.class, SFlowsCache.getCachedSflows().get(key));
					if (response.getStatus() != 200) {
						throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
					}
					String output = response.getEntity(String.class);
					SFlowsCache.getCachedSflows().remove(key);
					//logger.info("CachedSflows: " + SFlowsCache.getCachedSflows().get(key).getSflowsList());
					logger.info(output);
				}
//				Thread storeSflows = new Thread(new StoreSflows());
//				storeSflows.start();	
			}
			
			return "Sflow records processed!\n";
//		 	return null;
		} finally {
			SlaveStaticComponents.getLock().unlock();
		}
	}
	
	@Path("store/")
	@POST
	@Consumes(MediaType.APPLICATION_XML)
	public SflowsToStoreList writeSFlowRecords(SflowsList input) throws IOException {
		
		synchronized(myLock) {
			//get key and insert records to sflowsToStore HashMap
			String key = input.getSflowsList().get(input.getSflowsList().size() - 1);
			input.getSflowsList().remove(input.getSflowsList().size() - 1);
			logger.info("Key transferred: " + key);
			SFlowsCache.updateSflowsToStore(key, input);
		
			logger.info("SflowsToStore: " + SFlowsCache.getSflowsToStore().get(key).getSflowsList());
			//if HashMap is full store records to HDFS files
			if (SFlowsCache.fullSflowsToStore()) {
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(conf);
				Set<String> keySet = SFlowsCache.getSflowsToStore().keySet();
            
				//for each key store records in appropriate HDFS file
				for (String k : keySet) {
					//use key to open the correct file
					org.apache.hadoop.fs.Path pt = new org.apache.hadoop.fs.Path("hdfs://master:9000/opt/warehouse/" 
																			+ SlaveStaticComponents.getTableName() + "/part=" + k + "/part-" + k + ".gz");
					BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(fs.append(pt)), "UTF-8"));
					for (String record : SFlowsCache.getSflowsToStore().get(k).getSflowsList()) {
						bw.write(record);
						bw.newLine();
					}
					bw.close();
				}			
				//clean up SflowsToStore HashMap
				SFlowsCache.setSflowsToStore(new HashMap<String, SflowsList>());
			}
		
			//System.out.println(input.getCachedSflows().get("apple").getSflowsList());
			return new SflowsToStoreList(SFlowsCache.getSflowsToStore());
		}
	}
	
	@Path("flush/")
	@POST
	@Consumes(MediaType.APPLICATION_XML)
	public synchronized String flushSFlowRecords(String key) throws IOException {
		
		try {
			SlaveStaticComponents.getStoreLock().lock();
			logger.info("Writing remaining sFlows to an appropriate HDFS file");
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			org.apache.hadoop.fs.Path pt = new org.apache.hadoop.fs.Path("hdfs://master:9000/opt/warehouse/" 
																	+ SlaveStaticComponents.getTableName() + "/part=" + key + "/part-" + key + ".gz");
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(fs.append(pt)), "UTF-8"));
			for (String record : SFlowsCache.getSflowsToStore().get(key).getSflowsList()) {
				bw.write(record);
				bw.newLine();
			}
			bw.close();			
			
			SFlowsCache.deleteKeyFromSflowsToStore(key);
		    SlaveStaticComponents.getLock().unlock();
			
			return "Successfully flushed remaining sFlow records for file ID: " + key + "\n";
	} finally {
		SlaveStaticComponents.getStoreLock().unlock();
		}
	}
	
	/*
	@Path("test/")
	@POST
	@Consumes(MediaType.APPLICATION_XML)
	public synchronized String testAppend(String input) throws IOException {

			logger.info("Trying to append to HDFS");
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
//			org.apache.hadoop.fs.Path pt = new org.apache.hadoop.fs.Path("hdfs://master:9000/opt/warehouse" 
//																	+ SlaveStaticComponents.getTableName() + "/part=1/part-1.gz");
			org.apache.hadoop.fs.Path pt = new org.apache.hadoop.fs.Path("hdfs://master:9000/testFile");
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter((fs.append(pt))));
			bw.write(input);
			bw.newLine();
			bw.close();		
			
			return "Successfully appended to HDFS\n";
	}
	*/
}