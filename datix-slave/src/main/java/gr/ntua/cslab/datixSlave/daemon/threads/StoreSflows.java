package gr.ntua.cslab.datixSlave.daemon.threads;

import gr.ntua.cslab.datixSlave.daemon.cache.MappingCache;
import gr.ntua.cslab.datixSlave.daemon.cache.SFlowsCache;

import java.util.Set;

import org.apache.log4j.Logger;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class StoreSflows implements Runnable {
	public Logger logger = Logger.getLogger(StoreSflows.class);
	
	@Override
	public void run() {
		Set<String> keySet = SFlowsCache.getCachedSflows().keySet();
		Client client = Client.create();
		for (String key : keySet) {
				 	
			String slaveId = MappingCache.getFileMapping().get(""+key);
			logger.info("Sending sFlows for file: " + key + " to slave: " + slaveId);
			if (slaveId == null) {
				logger.fatal("Failed: No node assignment for the particular partition.");
			}
			WebResource webResource = client.resource("http://" + slaveId + ":8085/sflows/store");
			SFlowsCache.getCachedSflows().get(key).updateList(key);
			ClientResponse response = webResource.type("application/xml").post(ClientResponse.class, SFlowsCache.getCachedSflows().get(key));
			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
			}
//			String output = response.getEntity(String.class);
			SFlowsCache.getCachedSflows().remove(key);
			//logger.info("CachedSflows: " + SFlowsCache.getCachedSflows().get(key).getSflowsList());
//			logger.info(output);
		}
	}
}
