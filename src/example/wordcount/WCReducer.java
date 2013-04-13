package example.wordcount;

import java.util.Iterator;
import java.util.List;

import mapreduce.*;

public class WCReducer extends Reducer {

  @Override
  public void reduce(String key, Iterator<String> values, Outputer out) {
    long sum = 0;
    
   while(values.hasNext()) {
      sum += Long.parseLong(values.next());
    }
    
    out.collect(key, Long.toString(sum));
  }

}
