package example.wordcount;

import mapreduce.Partitioner;

public class WCPartitioner implements Partitioner {
  private int reducerNum;

  public WCPartitioner(Integer reducerNum) {
    this.reducerNum = reducerNum;
  }

  public int getPartition(String key) {
    return Math.abs(key.hashCode()) % this.reducerNum;
  }

  public int getReducerNum() {
    return this.reducerNum;
  }
}
