package example.degreecount;

import mapreduce.Mapper;
import mapreduce.Outputer;

public class DegreeCountMapper extends Mapper {

  @Override
  public void map(String key, String value, Outputer out) {
    out.collect(key, "out");
    out.collect(value, "in");
  }

}
