package example.wordcount;

public class WCPartitioner {
  private int reducerNum;

  public WCPartitioner(Integer reducerNum) {
    this.reducerNum = reducerNum;
  }

  public int getPartition(String key) {
    return key.hashCode() / this.reducerNum;
  }

  public int getReducerNum() {
    return this.reducerNum;
  }
}
