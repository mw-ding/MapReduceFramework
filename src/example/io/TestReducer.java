package example.io;

import java.util.List;

import mapreduce.*;

public class TestReducer extends Reducer {

  @Override
  public void reduce(String key, List<String> values, Outputer out) {
    for (String value : values) {
      out.collect(key, value);
    }
  }

}
