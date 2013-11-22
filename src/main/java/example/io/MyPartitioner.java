package example.io;

import mapreduce.Partitioner;


public class MyPartitioner implements Partitioner {
  private int reducerNum;

  public MyPartitioner(Integer reducerNum) {
    this.reducerNum = reducerNum;
  }

  public int getPartition(String key) {
    return Math.abs(key.hashCode()) % this.reducerNum;
  }

  public int getReducerNum() {
    return this.reducerNum;
  }
}
