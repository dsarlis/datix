package gr.ntua.cslab.streamnet.kdtree;

import gr.ntua.cslab.streamnet.cache.LeafPointsCache;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;



public class KdNode<T> implements Serializable {

	private static final long serialVersionUID = -1475019628722077172L;
	// All types
    protected int dimensions;
    protected int bucketCapacity;
    protected int size;
    protected boolean isLeaf;
    public int id, level;

    // Leaf only
    protected double[][] points;
    protected Object[] data;

    // Stem only
    protected KdNode<T> left, right;
    protected int splitDimension;
    protected double splitValue;

    // Bounds
    protected double[] minBound;
	protected double[] maxBound;
    protected double[] minBound1, maxBound1;
    protected boolean singlePoint;

    protected KdNode(BufferedReader br) throws IOException {
    	String line = br.readLine();
    	StringTokenizer tok = new StringTokenizer(line);
    	this.dimensions = Integer.parseInt(tok.nextToken());
    	this.bucketCapacity = Integer.parseInt(tok.nextToken());
    	readTree(br);
    }
    protected KdNode(BufferedReader br, int dimensions,int bucketCapacity) throws IOException {
    	this.dimensions = dimensions;
    	this.bucketCapacity=bucketCapacity;
    	readTree(br);
    }
	protected KdNode(int dimensions, int bucketCapacity) {
        // Init base
        this.dimensions = dimensions;
        this.bucketCapacity = bucketCapacity;
        this.size = 0;
        this.singlePoint = true;
        this.isLeaf = true;

        // Init leaf elements
        this.points = new double[bucketCapacity+1][];
        this.data = new Object[bucketCapacity+1];
        
        this.id=1;
        this.level=0;
        minBound1 = new double[dimensions];
        maxBound1 = new double[dimensions];
        for (int i = 0; i < dimensions; i++) {
        	minBound1[i]=Double.MIN_VALUE;
        	maxBound1[i]=Double.MAX_VALUE;
		}
    }
    protected KdNode(int dimensions, int bucketCapacity, int level, int id) {
        // Init base
        this.dimensions = dimensions;
        this.bucketCapacity = bucketCapacity;
        this.size = 0;
        this.singlePoint = true;
        this.isLeaf = true;

        // Init leaf elements
        this.points = new double[bucketCapacity+1][];
        this.data = new Object[bucketCapacity+1];
        
        this.id=id;
        this.level=level;
    }
    /* -------- SIMPLE GETTERS -------- */

    public int size() {
        return size;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    /* -------- OPERATIONS -------- */
    public boolean isLeaf1(int id) {
    	KdNode<T> cursor = this;
		
		if (cursor.isLeaf()) {
			if (cursor.id == id)
				return true;
			else 
				return false;
		}
		
		boolean l=false,r=false;
		if (cursor.left != null)
				l = left.isLeaf1(id);
		if (cursor.right != null) {
				r= right.isLeaf1(id);
		}
		
		return l || r;
    }
    
	public void findBuckets(double[] min, double[] max, List<Integer> l) {
		if(isLeaf()){
			l.add(id);
			return;
		}
		if(max[splitDimension] <= splitValue){
			left.findBuckets(min, max, l);
		}
		else if(min[splitDimension] > splitValue){
			right.findBuckets(min, max, l);
		}
		else{
			left.findBuckets(min, max, l);
			right.findBuckets(min, max, l);
		}
	}
	
	public int find1(double[] point) {
        KdNode<T> cursor = this;
        while (!cursor.isLeaf()) {
            //System.out.println(cursor.id);//+" splitDimension: "+cursor.splitDimension+" splitValue: "+cursor.splitValue);
            if (point[cursor.splitDimension] > cursor.splitValue) {
                cursor = cursor.right;
            }
            else {
                cursor = cursor.left;
            }
        }
        
        /*System.out.println(cursor.id+": ");
        for (int i = 0; i < cursor.dimensions; i++) {
			System.out.println("min: "+cursor.minBound1[i]+" max: "+cursor.maxBound1[i]);
		}*/
		return cursor.id;
	}
	
    /*public void addPoint(int id, double[] point, T value) {
        KdNode<T> cursor = this;
        if (cursor.isLeaf()) {
        	if (cursor.id == id) {
        		LeafPointsCache.addPoint(id, point);
        	}
        	return;
        }
        
		if (cursor.left != null)
				left.addPoint(id, point, value);
		if (cursor.right != null) {
				right.addPoint(id, point, value);
		}
    }*/

    /* -------- INTERNAL OPERATIONS -------- */

    public KdNode<T> calculateSplit(int id) {
    	KdNode<T> cursor = this;
		
		if (cursor.isLeaf()) {
			if (cursor.id == id) {
				if (cursor.singlePoint) return null;

		        double width = 0;
		        for (int i = 0; i < cursor.dimensions; i++) {
		            double dwidth = (cursor.maxBound[i] - cursor.minBound[i]);
		            if (Double.isNaN(dwidth)) dwidth = 0;
		            if (dwidth > width) {
		                cursor.splitDimension = i;
		                width = dwidth;
		            }
		        }

		        if (width == 0) {
		            return null;
		        }

		        // Start the split in the middle of the variance
		        //splitValue = (minBound[splitDimension] + maxBound[splitDimension]) * 0.5;
		        List<Double> l = new ArrayList<Double>();
		        ArrayList<double[]> cursorPoints = LeafPointsCache.getPoints().get(id);
		        for (double[] point  : cursorPoints) {
					l.add(point[cursor.splitDimension]);
				}
		        Collections.sort(l);
		        cursor.splitValue = l.get(cursor.bucketCapacity/2);
		        
		        
		        // Never split on infinity or NaN
		        if (cursor.splitValue == Double.POSITIVE_INFINITY) {
		            cursor.splitValue = Double.MAX_VALUE;
		        }
		        else if (cursor.splitValue == Double.NEGATIVE_INFINITY) {
		            cursor.splitValue = -Double.MAX_VALUE;
		        }

		        // Don't let the split value be the same as the upper value as
		        // can happen due to rounding errors!
		        if (cursor.splitValue == maxBound[cursor.splitDimension]) {
		            cursor.splitValue = minBound[cursor.splitDimension];
		        }

		        // Success
		        return cursor;
			}
			else
				return null;
		}
		
		KdNode<T> l=null,r=null;
		if (cursor.left != null)
				l = left.calculateSplit(id);
		if (cursor.right != null) {
				r= right.calculateSplit(id);
		}
		if (l == null)
			return r;
		else
			return l;
    }
    
    public double[] splitLeafNode(KdNode<T> cursor) {
		
		int nextLevelStart = (int)Math.pow(2, cursor.level+1);
		int thisLevelOffset = id - (int)Math.pow(2, cursor.level);
		int nextId= nextLevelStart + thisLevelOffset*2;
		double[] minBoundsNext1 = new double[cursor.dimensions];
		double[] maxBoundsNext1 = new double[cursor.dimensions];
		double[] minBoundsNext2 = new double[cursor.dimensions];
		double[] maxBoundsNext2 = new double[cursor.dimensions];
		for (int i = 0; i < cursor.dimensions; i++) {
			minBoundsNext1[i]=cursor.minBound1[i];
			minBoundsNext2[i]=cursor.minBound1[i];
			maxBoundsNext1[i]=cursor.maxBound1[i];
		    maxBoundsNext2[i]=cursor.maxBound1[i];
		    if(i==cursor.splitDimension){
		    		maxBoundsNext1[i]=cursor.splitValue;
		    		minBoundsNext2[i]=cursor.splitValue;
		    }
		}
		cursor.right = new KdNode<T>(cursor.dimensions, cursor.bucketCapacity,cursor.level+1,nextId);
		cursor.left = new KdNode<T>(cursor.dimensions, cursor.bucketCapacity,cursor.level+1,nextId+1);
		cursor.left.minBound1=minBoundsNext1;
		cursor.left.maxBound1=maxBoundsNext1;
		cursor.right.minBound1=minBoundsNext2;
		cursor.right.maxBound1=maxBoundsNext2;

		// Move locations into children
		LeafPointsCache.splitPoints(id, nextId+1, nextId, cursor.splitDimension, cursor.splitValue);

		cursor.points = null;
		cursor.data = null;
		cursor.isLeaf = false;
		return new double[]{id, nextId+1, nextId, splitValue, splitDimension};
    }
    
    public void printTree(){
    	
    	System.out.print(level+" "+id+" ");
    	if(isLeaf()){
        	System.out.println(this.size);
    	}
    	else{
    		System.out.println();
    	}
    	if(left!=null)
    		left.printTree();
    	if(right!=null)
    		right.printTree();
    }
    
	public void printTree1(BufferedWriter writer) throws IOException {
		printNode(writer);
    	if(left!=null){
    		left.printTree1(writer);
    		right.printTree1(writer);
    	}
		
	}

    
    private void readTree(BufferedReader br) throws IOException {
    	readNode(br);
    	if(!isLeaf()){
    		left = new KdNode<T>(br,dimensions,bucketCapacity);
    		right = new KdNode<T>(br,dimensions,bucketCapacity);
    	}
		
	}
    private void readNode(BufferedReader br) throws IOException {

    	String line = br.readLine();
    	StringTokenizer tok = new StringTokenizer(line);
    	this.level = Integer.parseInt(tok.nextToken());
    	this.id = Integer.parseInt(tok.nextToken());
    	this.splitDimension = Integer.parseInt(tok.nextToken());
    	this.splitValue = Double.parseDouble(tok.nextToken());
    	this.isLeaf = Boolean.parseBoolean(tok.nextToken());
    	if(isLeaf){
    		this.points = new double[dimensions][dimensions];
    	}
    	this.minBound1 = new double[dimensions];
    	this.maxBound1 = new double[dimensions];
		for (int i = 0; i < dimensions; i++) {
			
			this.minBound1[i]=Double.parseDouble(tok.nextToken());
			this.maxBound1[i]=Double.parseDouble(tok.nextToken());
		}
	}   
    private void printNode(BufferedWriter writer) throws IOException {
		writer.write(this.level+" ");
		writer.write(this.id+" ");
		writer.write(this.splitDimension+" ");
		writer.write(this.splitValue+" ");
		writer.write(this.isLeaf + " ");
		for (int i = 0; i < dimensions; i++) {
			writer.write(this.minBound1[i]+" "+this.maxBound1[i]+" ");
		}
		writer.write("\n");
	}
	public int countLeafs(){
    	if(isLeaf())
    		return 1;
    	else 
    		return left.countLeafs() + right.countLeafs();
	}
}