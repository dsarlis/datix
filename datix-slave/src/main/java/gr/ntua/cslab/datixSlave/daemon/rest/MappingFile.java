package gr.ntua.cslab.datixSlave.daemon.rest;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import gr.ntua.cslab.datixSlave.daemon.cache.MappingCache;
import gr.ntua.cslab.datixSlave.daemon.cache.SFlowsCache;
import gr.ntua.cslab.datixSlave.daemon.shared.SlaveStaticComponents;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

@Path("/mappingFile/")
public class MappingFile {
	public Logger logger = Logger.getLogger(MappingFile.class);
	
	@Path("update/")
	@POST
	@Consumes(MediaType.APPLICATION_XML)
	public synchronized String updateMappingFile(String input) throws FileNotFoundException, IOException {
		
		SlaveStaticComponents.getLock().lock();
		String[] parts = input.split(" ");
		String keyLeft = parts[0];
		String valueLeft = parts[1];
		String keyRight = parts[2];
		String valueRight = parts[3];
		String initialKey = parts[4];
		
		MappingCache.updateMapping(keyLeft, valueLeft, keyRight, valueRight, initialKey);
		logger.info("Mapping File: Slave ID for key: " + keyLeft + " is " + MappingCache.getFileMapping().get(keyLeft));
		logger.info("Mapping File: Slave ID for key: " + keyRight + " is " + MappingCache.getFileMapping().get(keyRight));
		
//		make a backup of mapping file
		ObjectOutputStream s = new ObjectOutputStream(new FileOutputStream(SlaveStaticComponents.getMappingFile()));
		s.writeObject(MappingCache.getFileMapping());
		s.close();
		logger.info("Made a back up of Mapping File");
		
		if (SFlowsCache.getSflowsToStore().containsKey(initialKey)) {
			SlaveStaticComponents.setLocked(true);
		}
		
		return "Mapping File updated!\n";
	}
	
	@Path("print/")
	@GET
	public synchronized String printMappingFile() {
		
		logger.info("******** Start of Mapping File ********");
		for (String key : MappingCache.getFileMapping().keySet()) {
			logger.info("key: " + key + " value: " + MappingCache.getFileMapping().get(key));
		}
		logger.info("******** End of Mapping File ********");
		
		return "Printed Mapping File\n";
	}
}
