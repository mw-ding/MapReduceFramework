package example.wordcount;

import java.util.List;

import mapreduce.*;

public class WCReducer extends Reducer {

  @Override
  public void reduce(String key, List<String> values, Outputer out) {
    long sum = 0;
    
    for (String value : values) {
      sum += Long.parseLong(value);
    }
    
    out.collect(key, Long.toString(sum));
  }

}
