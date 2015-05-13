package gr.ntua.cslab.datix.client;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricsAggr  extends TimerTask {
	private BufferedWriter  bw;
	private int timeInterval;
	
	public MetricsAggr(BufferedWriter bw, int timeInterval) {
		TestClient.sFlowCount = new AtomicInteger(0);
		this.bw = bw;
		this.timeInterval = timeInterval;
	}
	
	@Override
	public void run() {
		int sFlowCount = TestClient.sFlowCount.get();
		try {
			//time interval given in ms
			bw.write(""+ ((double) sFlowCount / timeInterval) * 1000);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			bw.newLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		System.out.println("Throughput: " + ((double) sFlowCount / 5) +" records/sec");
		TestClient.sFlowCount.set(0);
	}

}
