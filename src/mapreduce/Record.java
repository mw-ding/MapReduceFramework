package mapreduce;

public class Record implements Comparable<Record>{
  public String key;
  public String value;
  public Record(String key, String value){
    this.key = key;
    this.value = value;
  }
  @Override
  public int compareTo(Record o) {
    return this.key.compareTo(o.key);
  }
}
