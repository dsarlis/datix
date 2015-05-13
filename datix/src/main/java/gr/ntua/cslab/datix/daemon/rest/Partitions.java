package gr.ntua.cslab.datix.daemon.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.apache.log4j.Logger;

@Path("/partitions/{id}")
public class Partitions {
	
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
	
	@GET
	public String getPartition(@PathParam("id") int id) throws IOException, InterruptedException {
		//String[] command = new String[] {"hive", "-e", "Query String"};
		//executeCommand(command);
		return "Partition Number: " + id;
	}
	
	@GET
	@Path("add")
	public String addPartition(@PathParam("id") int id) throws IOException, InterruptedException {
		String[] command = new String[] {"hive", "-e", "Query String"};
		executeCommand(command);
		return "Partition No. " + id + " added!";
	}
	
	@GET
	@Path("delete")
	public String deletePartition(@PathParam("id") int id) throws IOException, InterruptedException {
		String[] command = new String[] {"hive", "-e", "Query String"};
		executeCommand(command);
		return "Partition No. " + id + " deleted!";
	}
}
