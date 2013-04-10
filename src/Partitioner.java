public class Partitioner {
  int reducerNum;

  public Partitioner(int reducerNum) {
    this.reducerNum = reducerNum;
  }

  public int getPartition(String key) {
    return key.hashCode() / this.reducerNum;
  }
}
