package gr.ntua.cslab.datix.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class TestClient {
	public static AtomicInteger sFlowCount;
	
	private static class SendSFlowRecords implements Runnable {
		private String records;
		private int slaveId;
		private int batchSize;
		
		public SendSFlowRecords(String records, int slaveId, int batchSize) {
			this.records = records;
			this.slaveId = slaveId;
			this.batchSize = batchSize;
		}
		
		public void run() {
			try {
				if (records.length() != 0) {
					System.out.println("Sending records to slave: " + slaveId);
					Client client = Client.create();
					WebResource webResource = client.resource("http://slave" + slaveId + ":8085/sflows/dispatch");
					ClientResponse response = webResource.type("application/xml").post(ClientResponse.class, records);
					if (response.getStatus() != 200) {
						throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
					}
					TestClient.sFlowCount.getAndAdd(batchSize);
					//String output = response.getEntity(String.class);
					//System.out.println(output);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private static class SendSamples implements Runnable {
		private String record;
		
		public SendSamples(String record) {
			this.record = record;
		}
		
		public void run() {
			try {
					System.out.println("Sending sample record to master");
					Client client = Client.create();
					WebResource webResource = client.resource("http://master:8085/kdtree/update");
					ClientResponse response = webResource.type("application/xml").post(ClientResponse.class, record);
					if (response.getStatus() != 200) {
						throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
					}
					//String output = response.getEntity(String.class);
					//System.out.println(output);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException, FileNotFoundException, IOException {
		 
		if (args.length < 6) {
			System.out.println("Wrong number of arguments. Example usage: TestClient <batchSize> <threadNum> "
												+ "<sampleRate> <timeInterval> <input_file> <metricsFile>");
			System.exit(-1);
		}
		
		int batchSize = Integer.parseInt(args[0]);
		int threadNum = Integer.parseInt(args[1]);
		System.out.println("batchSize: " + batchSize + " threadNum: " + threadNum);
		double sampleRate = Double.parseDouble(args[2]);
		int timeInterval = Integer.parseInt(args[3]);
//		ArrayList<Thread> workers = new ArrayList<Thread>();
		
		String records = "";
		int slaveId = 1;
		BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(args[4])), "UTF-8"));
		String line = br.readLine();
		ExecutorService executor = Executors.newFixedThreadPool(threadNum);
		int count = 0;
		BufferedWriter bw = new BufferedWriter(new FileWriter(args[5]));
		TimerTask timerTask = new MetricsAggr(bw, timeInterval);
        Timer timer = new Timer(true);
        //set time interval: <timerTask, initialTime, timeInterval in ms>
        timer.scheduleAtFixedRate(timerTask, 0, timeInterval); 
		while (line  != null) {
//				for (int j = 0; j < threadNum; j++) {
					int i = 0;
					records = "";
					while (i < batchSize && line != null) {
						count++;
						Random ran = new Random();
						if (ran.nextDouble() < sampleRate)  {
							Thread master = new Thread(new SendSamples(line));
							master.start();
						}
						records += line + ",";
						line = br.readLine();
						i++;
					}
					Thread worker = new Thread(new SendSFlowRecords(records, slaveId, batchSize));
					slaveId++;
					if (slaveId == 15)
						slaveId = 1;
					executor.execute(worker);
//					workers.add(j, worker)
//					worker.start();
//				}
//				for (int j = 0; j < threadNum; j++) {
//					workers.get(j).join();
//				}
				System.out.println("sFlow count: " + count);
		}
		br.close();
		executor.shutdown();
		while (!executor.isTerminated()) {}
		System.out.println("All threads finished");
		bw.close();
	}
}
