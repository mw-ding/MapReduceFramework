public class Partitioner {
  private int reducerNum;

  public Partitioner(Integer reducerNum) {
    this.reducerNum = reducerNum;
  }

  public int getPartition(String key) {
    return key.hashCode() % this.reducerNum;
  }

  public int getReducerNum() {
    return this.reducerNum;
  }
}
