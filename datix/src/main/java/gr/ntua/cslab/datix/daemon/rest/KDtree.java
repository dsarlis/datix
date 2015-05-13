package gr.ntua.cslab.datix.daemon.rest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.zip.GZIPInputStream;
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

import gr.ntua.cslab.datix.daemon.cache.*;
import gr.ntua.cslab.datix.daemon.shared.ServerStaticComponents;

@Path("/kdtree/")
public class KDtree {

    public Logger logger = Logger.getLogger(KDtree.class);
    
    public Map<String,String> executeCommand(String[] command) throws IOException, InterruptedException {
		String c="Executing command: ";
		for (int i = 0; i < command.length; i++) {
			c+=command[i]+" ";
		}
		logger.info(c);
		
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
        logger.info("Command Output: "+output.toString());
        ret.put("output", output.toString());
		reader = new BufferedReader(new InputStreamReader(p1.getErrorStream()));
		line = "";	
		output = new StringBuffer();		
		while ((line = reader.readLine())!= null) {
			output.append(line + "\n");
		}
        logger.info("Command Error: "+output.toString());
        ret.put("error", output.toString());
		return ret;
 
	}
    
    @Path("create/")
    public synchronized String createKDTree() throws IOException, Exception {
    	
    	return "Created KDtree";
    }
    
    @Path("update/")
    @POST
    @Consumes({MediaType.APPLICATION_XML})
    public synchronized String changeKDTree(String input) throws IOException, Exception {
    	//create point from appropriate SFlow record fields
      try {
    	ServerStaticComponents.getLock().lock();
    	String[] parts = input.split(" ");
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
							 	logger.error("Failed: Date not in compatible format. Example: 2015-02-18");
								e.printStackTrace();
							 }
		                     break;
		                     
		            default: logger.error("Dimension number " + Integer.parseInt(KDtreeCache.getDimensions()[i]));
		            		 break;
		        }
			 }
			double[] idz = KDtreeCache.getKd().addPoint(point, (long) 100);
			BufferedWriter writer = new BufferedWriter(new FileWriter(ServerStaticComponents.getKdTreeFile()));
			KDtreeCache.getKd().printTree(writer);
			//if a split has been performed: 
			if ((int) idz[0] != -1) {
				//1: send new mapping File to slaves
				try {
					logger.info("Send Mapping File to slaves...");
					Client client = Client.create();
					WebResource webResource = client.resource("http://localhost:8085/mappingFile/update");
					Random ran = new Random();
					String mapping = "" + idz[2] + " " + ServerStaticComponents.getSlaves()[ran.nextInt(14) + 1] + " " + idz[1] + " " 
												   + ServerStaticComponents.getSlaves()[ran.nextInt(14 ) + 1] + " " + idz[0];
					logger.info("New mapping: " + mapping);
					ClientResponse response = webResource.type("application/xml").post(ClientResponse.class, mapping);
					if (response.getStatus() != 200) {
						throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
					}
					String output = response.getEntity(String.class);
//					System.out.println(output);
					logger.info(output);
				} catch (Exception e) {
					e.printStackTrace();
				}
				String slaveId = MappingCache.getFileMapping().get("" + idz[0]);
				if (slaveId == null) {
					//should not reach here
					logger.fatal("Internal error: No slave assigned for file: " + idz[0]);
				}
				try {
					logger.info("Write remaining sFlows to old partition file");
					Client client = Client.create();
					WebResource webResource = client.resource("http://" + slaveId + ":8085/sflows/flush");
					ClientResponse response = webResource.type("application/xml").post(ClientResponse.class, "" + idz[0]);
					if (response.getStatus() != 200) {
						throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
					}
					String output = response.getEntity(String.class);
//					System.out.println(output);
					logger.info(output);
				} catch (Exception e) {
					e.printStackTrace();
				}
				//2: send new K-d Tree to slaves
				try {
					logger.info("Send Kd Tree to slaves");
					Client client = Client.create();
					InputStream input1 = new FileInputStream(ServerStaticComponents.getKdTreeFile());
				
					for (String slave : ServerStaticComponents.getSlaves()) {
						logger.info("Sending Kd Tree to slave: " + slave);
						WebResource webResource = client.resource("http://" + slave + ":8085/kdtree/update");
						ClientResponse response = webResource.type("application/octet-stream").post(ClientResponse.class, input1);
						if (response.getStatus() != 200) {
							throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
						}
						String output = response.getEntity(String.class);
//						System.out.println(output);
						logger.info(output);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				//3: add new partitions to Hive
				logger.info("Adding new partitions to Hive table");
				String[] addCommand = new String[] {"hive", "-e", "ALTER TABLE " + 
						ServerStaticComponents.getTableName() +" ADD PARTITION (part = '" + (int) idz[1] + "') location 'part=" + (int) idz[1] + "';"};
				executeCommand(addCommand);
				addCommand = new String[] {"hive", "-e", "ALTER TABLE " + 
						ServerStaticComponents.getTableName() +" ADD PARTITION (part = '" + (int) idz[2] + "') location 'part=" + (int) idz[2] + "';"};
				
				//4: copy data to new partitions
				logger.info("Copying data to the appropriate new partitions");
				org.apache.hadoop.fs.Path pt = new org.apache.hadoop.fs.Path("hdfs://master:9000/opt/warehouse/" 
																		+ ServerStaticComponents.getTableName() + "/part=" + (int) idz[0] + "/part-" + (int) idz[0] + ".gz");
                FileSystem fs = FileSystem.get(new Configuration());
				InputStream fileStream = fs.open(pt);
	            InputStream gzipStream = new GZIPInputStream(fileStream);
	            Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
	            BufferedReader br = new BufferedReader(new BufferedReader(decoder));
	            org.apache.hadoop.fs.Path ptLeft = new org.apache.hadoop.fs.Path("hdfs://master:9000/opt/warehouse/" 
																			   + ServerStaticComponents.getTableName() + "/part=" + (int) idz[2] + "/part-" + (int) idz[2] + ".gz");
	            logger.info(ptLeft.toString());
            	BufferedWriter bwLeft = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(fs.append(ptLeft))));
            	org.apache.hadoop.fs.Path ptRight = new org.apache.hadoop.fs.Path("hdfs://master:9000/opt/warehouse/" 
																				 + ServerStaticComponents.getTableName() + "/part=" + (int) idz[1] + "/part-" + (int) idz[1] + ".gz");
            	logger.info(ptRight.toString());
            	BufferedWriter bwRight = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(fs.append(ptRight))));
	            String line;
	            double splitValue = idz[3];
	            int splitDimension = Integer.parseInt(KDtreeCache.getDimensions()[(int) idz[4]]);
				while ((line = br.readLine()) != null) {
					String[] parts1 = line.split(" ");
					double value = 0;
					
					switch (splitDimension) {
						case 1:	value = Double.parseDouble(parts1[1]);
							break;
						case 3:	value = Double.parseDouble(parts1[3]);
						case 8:	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
						 				try {
						 						Date dateStr = formatter.parse(parts1[8]);
						 						value = (double) dateStr.getTime();
						 				} catch (ParseException e) {
						 						logger.error("Failed: Date not in compatible format. Example: 2015-02-18");
						 						e.printStackTrace();
						 				}
						 				break;
		                     
						default: logger.error("Dimension number " + splitDimension);
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
            	
				//5: delete the old partition
            	logger.info("Deleting the old partition");
				String[] dropCommand = new String[] {"hive", "-e", "ALTER TABLE " + 
														 	ServerStaticComponents.getTableName() + " DROP PARTITION (part = '"+ (int) idz[0] + "');"};
				executeCommand(dropCommand);
				
				return "KDtree changed. A split has been performed in node: " + (int) idz[0] + 
	    				  	  "\nTwo nodes created with ID: " + (int) idz[1] + "," + (int) idz[2]+ "\n";
			}
			logger.info("Kd Tree updated. No split performed!");
			return "KDtree changed. No split performed!\n";
		 }
    }finally{
			ServerStaticComponents.getLock().unlock();
		}
//		should not reach this point
    	return "KDtree not changed";
    }
}