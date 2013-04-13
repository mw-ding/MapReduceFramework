package example.io;

import java.util.Iterator;
import java.util.List;

import mapreduce.*;

public class TestReducer extends Reducer {

  @Override
  public void reduce(String key, Iterator<String> values, Outputer out) {
    while (values.hasNext()) {
      out.collect(key, values.next());
    }
  }

}
