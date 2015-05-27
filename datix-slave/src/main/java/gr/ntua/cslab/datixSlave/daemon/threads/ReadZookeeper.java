package gr.ntua.cslab.datixSlave.daemon.threads;

import java.io.IOException;

import gr.ntua.cslab.datixSlave.daemon.zookeeper.Read;

import org.apache.log4j.Logger;

public class ReadZookeeper implements Runnable {
	public Logger logger = Logger.getLogger(ReadZookeeper.class);
	
	@Override
	public void run() {
		Read rt = new Read("master:2181", "/datix");
			logger.info("Starting reading K-d Tree and Mapping File into memory...");
			try {
				rt.read();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

}
