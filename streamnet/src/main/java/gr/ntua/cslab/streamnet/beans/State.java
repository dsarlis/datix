package gr.ntua.cslab.streamnet.beans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Logger;

import gr.ntua.cslab.streamnet.kdtree.KdTree;
import gr.ntua.cslab.streamnet.zookeeper.SyncPrimitive;

public class State implements Serializable {
	private static final long serialVersionUID = 4370513757672577183L;
	private Map<String, String> fileMapping;
	private Map<Integer, ArrayList<double[]>> points;
	private KdTree<Long> kd;
	
	public State(Map<String, String> fileMapping,
			Map<Integer, ArrayList<double[]>> points, KdTree<Long> kd) {
		super();
		this.fileMapping = fileMapping;
		this.points = points;
		this.kd = kd;
	}
	
	public Map<String, String> getFileMapping() {
		return fileMapping;
	}
	public void setFileMapping(Map<String, String> fileMapping) {
		this.fileMapping = fileMapping;
	}
	
	public Map<Integer, ArrayList<double[]>> getPoints() {
		return points;
	}
	public void setPoints(Map<Integer, ArrayList<double[]>> points) {
		this.points = points;
	}
	
	public KdTree<Long> getKd() {
		return kd;
	}
	public void setKd(KdTree<Long> kd) {
		this.kd = kd;
	}
}
