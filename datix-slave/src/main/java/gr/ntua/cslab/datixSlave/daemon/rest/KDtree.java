package gr.ntua.cslab.datixSlave.daemon.rest;

import gr.ntua.cslab.datixSlave.daemon.cache.KDtreeCache;
//import gr.ntua.cslab.datixSlave.daemon.shared.SlaveStaticComponents;

import gr.ntua.cslab.datixSlave.daemon.shared.SlaveStaticComponents;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import ags.utils.dataStructures.trees.thirdGenKD.KdTree;

@Path("/kdtree/")
public class KDtree {
	
	public Logger logger = Logger.getLogger(KDtree.class);
	
	@Path("update/")
    @POST
    @Produces({MediaType.APPLICATION_XML})
    @Consumes({MediaType.APPLICATION_OCTET_STREAM})
    public synchronized String updateKDTree(InputStream input) throws IOException, Exception {

//		synchronized(SlaveStaticComponents.getLock()) {
//		SlaveStaticComponents.getLock().writeLock().lock();
//		try {
			KDtreeCache.setKd(new KdTree<Long>(new BufferedReader(new InputStreamReader(input))));
//		}
//		finally{
//			SlaveStaticComponents.getLock().writeLock().unlock();
//		}
        
        BufferedWriter writer = new BufferedWriter(new FileWriter(SlaveStaticComponents.getKdTreeFile()));
        KDtreeCache.getKd().printTree(writer);
        logger.info("KD Tree written");
        writer.close();
        input.close();
        
        if (!SlaveStaticComponents.isLocked()) {
        	SlaveStaticComponents.setLocked(false);
        	SlaveStaticComponents.getLock().unlock();
        }
        
    	return "File received";
//		}
    }
}
