package gr.ntua.cslab.datix.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TestLock {
	private final List<Integer> list = new ArrayList<Integer>();
	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	private final Lock r = rwl.readLock();
	private final Lock w = rwl.writeLock();
	
	public int get(int index) {
		r.lock();
		try {
			int result = list.get(index);
			System.out.println("read: " + result);
			return result;
		}
		catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
		finally {
			r.unlock();
		}
	}
	
	public void put(int newEntry) {
		w.lock();
		try {
			list.add(newEntry);
			System.out.println("added: " + newEntry);
		}
		finally {
			w.unlock();
		}
	}
	
	public static void main(String[] args) {
	
		TestLock worker = new TestLock();
		for (int i = 0; i < 10; i++) {
			worker.put(i);
		}	
		worker.get(2);
		worker.get(4);
		worker.put(100);
		worker.get(5);
	}
}
