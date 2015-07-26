package gr.ntua.cslab.streamnet.cache;

import java.util.ArrayList;
import java.util.Map;

public class LeafPointsCache {
	private static Map <Integer, ArrayList<double[]>> points;

	public static synchronized Map<Integer, ArrayList<double[]>> getPoints() {
		return points;
	}

	public static synchronized void setPoints(Map<Integer, ArrayList<double[]>> points) {
		LeafPointsCache.points = points;
	}
	
	public static synchronized void addPoint(int id, double[] point) {
		ArrayList<double[]> currPoints = LeafPointsCache.points.get(id);
		if (currPoints == null) {
			ArrayList<double[]> newPoints = new ArrayList<double[]>();
			newPoints.add(point);
			LeafPointsCache.points.put(id, newPoints);
		}
		else {
			currPoints.add(point);
			LeafPointsCache.points.put(id, currPoints);
		}
	}
	
	public static synchronized void deletePoints(int id) {
		LeafPointsCache.points.remove(id);
	}
	
	public static synchronized void splitPoints(int id, int idLeft, int idRight, 
			int splitDimension, double splitValue) {
//		System.out.println("Requesting ids: " + id + " " + idLeft + " " + idRight + " keyset: " + points.keySet());
		ArrayList<double[]> prevPoints = LeafPointsCache.getPoints().get(id);
		for (double[] point: prevPoints) {
			if (point[splitDimension] > splitValue) {
				LeafPointsCache.addPoint(idRight, point);
			}
			else {
				LeafPointsCache.addPoint(idLeft, point);
			}
		}
	}
}
