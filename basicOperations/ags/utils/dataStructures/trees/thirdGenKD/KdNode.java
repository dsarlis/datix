package ags.utils.dataStructures.trees.thirdGenKD;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import javax.swing.plaf.SliderUI;

/**
 *
 */
class KdNode<T> {
    // All types
    protected int dimensions;
    protected int bucketCapacity;
    protected int size;
    public int id, level;

    // Leaf only
    protected double[][] points;
    protected Object[] data;

    // Stem only
    protected KdNode<T> left, right;
    protected int splitDimension;
    protected double splitValue;

    // Bounds
    protected double[] minBound, maxBound;
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
    	this.dimensions=dimensions;
    	this.bucketCapacity=bucketCapacity;
    	readTree(br);
    }
	protected KdNode(int dimensions, int bucketCapacity) {
        // Init base
        this.dimensions = dimensions;
        this.bucketCapacity = bucketCapacity;
        this.size = 0;
        this.singlePoint = true;

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
        return points != null;
    }

    /* -------- OPERATIONS -------- */
	public void findBuckets(double[] min, double[] max, List<Integer> l) {
		if(isLeaf()){
			l.add(id);
			return;
		}
		boolean found = true;
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
	
    public void addPoint(double[] point, T value) {
        KdNode<T> cursor = this;
        while (!cursor.isLeaf()) {
            cursor.extendBounds(point);
            cursor.size++;
            if (point[cursor.splitDimension] > cursor.splitValue) {
                cursor = cursor.right;
            }
            else {
                cursor = cursor.left;
            }
        }
        cursor.addLeafPoint(point, value);
    }

    /* -------- INTERNAL OPERATIONS -------- */

    public void addLeafPoint(double[] point, T value) {
        // Add the data point
        points[size] = point;
        data[size] = value;
        extendBounds(point);
        size++;

        if (size == points.length - 1) {
            // If the node is getting too large
            if (calculateSplit()) {
                // If the node successfully had it's split value calculated, split node
                splitLeafNode();
            } else {
                // If the node could not be split, enlarge node
                increaseLeafCapacity();
            }
        }
    }

    private boolean checkBounds(double[] point) {
        for (int i = 0; i < dimensions; i++) {
            if (point[i] > maxBound[i]) return false;
            if (point[i] < minBound[i]) return false;
        }
        return true;
    }

    private void extendBounds(double[] point) {
        if (minBound == null) {
            minBound = Arrays.copyOf(point, dimensions);
            maxBound = Arrays.copyOf(point, dimensions);
            return;
        }

        for (int i = 0; i < dimensions; i++) {
            if (Double.isNaN(point[i])) {
                if (!Double.isNaN(minBound[i]) || !Double.isNaN(maxBound[i])) {
                    singlePoint = false;
                }
                minBound[i] = Double.NaN;
                maxBound[i] = Double.NaN;
            }
            else if (minBound[i] > point[i]) {
                minBound[i] = point[i];
                singlePoint = false;
            }
            else if (maxBound[i] < point[i]) {
                maxBound[i] = point[i];
                singlePoint = false;
            }
        }
    }

    private void increaseLeafCapacity() {
        points = Arrays.copyOf(points, points.length*2);
        data = Arrays.copyOf(data, data.length*2);
    }

    private boolean calculateSplit() {
        if (singlePoint) return false;

        double width = 0;
        for (int i = 0; i < dimensions; i++) {
            double dwidth = (maxBound[i] - minBound[i]);
            if (Double.isNaN(dwidth)) dwidth = 0;
            if (dwidth > width) {
                splitDimension = i;
                width = dwidth;
            }
        }

        if (width == 0) {
            return false;
        }

        // Start the split in the middle of the variance
        //splitValue = (minBound[splitDimension] + maxBound[splitDimension]) * 0.5;
        List<Double> l = new ArrayList<Double>(); 
        for (int i = 0; i < size; i++) {
			l.add(points[i][splitDimension]);
		}
        Collections.sort(l);
        splitValue = l.get(bucketCapacity/2);
        
        
        // Never split on infinity or NaN
        if (splitValue == Double.POSITIVE_INFINITY) {
            splitValue = Double.MAX_VALUE;
        }
        else if (splitValue == Double.NEGATIVE_INFINITY) {
            splitValue = -Double.MAX_VALUE;
        }

        // Don't let the split value be the same as the upper value as
        // can happen due to rounding errors!
        if (splitValue == maxBound[splitDimension]) {
            splitValue = minBound[splitDimension];
        }

        // Success
        return true;
    }

    private void splitLeafNode() {
    	int nextLevelStart = (int)Math.pow(2, level+1);
    	int thisLevelOffset = id - (int)Math.pow(2, level) ;
    	int nextId= nextLevelStart + thisLevelOffset*2;
    	double[] minBoundsNext1 = new double[dimensions];
    	double[] maxBoundsNext1 = new double[dimensions];
    	double[] minBoundsNext2 = new double[dimensions];
    	double[] maxBoundsNext2 = new double[dimensions];
    	for (int i = 0; i < dimensions; i++) {
    		minBoundsNext1[i]=minBound1[i];
    		minBoundsNext2[i]=minBound1[i];
    		maxBoundsNext1[i]=maxBound1[i];
    		maxBoundsNext2[i]=maxBound1[i];
    		if(i==splitDimension){
    			maxBoundsNext1[i]=splitValue;
    			minBoundsNext2[i]=splitValue;
    		}
		}
        right = new KdNode<T>(dimensions, bucketCapacity,level+1,nextId);
        left = new KdNode<T>(dimensions, bucketCapacity,level+1,nextId+1);
        left.minBound1=minBoundsNext1;
        left.maxBound1=maxBoundsNext1;
        right.minBound1=minBoundsNext2;
        right.maxBound1=maxBoundsNext2;

        // Move locations into children
        for (int i = 0; i < size; i++) {
            double[] oldLocation = points[i];
            Object oldData = data[i];
            if (oldLocation[splitDimension] > splitValue) {
                right.addLeafPoint(oldLocation, (T) oldData);
            }
            else {
                left.addLeafPoint(oldLocation, (T) oldData);
            }
        }

        points = null;
        data = null;
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
    	boolean isLeaf = Boolean.parseBoolean(tok.nextToken());
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
		writer.write(this.isLeaf()+" ");
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