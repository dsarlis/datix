package gr.ntua.cslab.streamnet.kdtree;

public interface MinHeap<T> {
    public int size();
    public void offer(double key, T value);
    public void replaceMin(double key, T value);
    public void removeMin();
    public T getMin();
    public double getMinKey();
}