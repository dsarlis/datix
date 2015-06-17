package gr.ntua.cslab.streamnet.beans;

public class PartitionInfo {
	private int partitionId;
	private double[] point;
	
	public PartitionInfo(int partitionId, double[] point) {
		super();
		this.partitionId = partitionId;
		this.point = point;
	}

	public int getPartitionId() {
		return partitionId;
	}

	public void setPartitionId(int partitionId) {
		this.partitionId = partitionId;
	}

	public double[] getPoint() {
		return point;
	}

	public void setPoint(double[] point) {
		this.point = point;
	}
}
