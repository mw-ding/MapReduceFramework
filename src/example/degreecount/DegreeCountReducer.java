package example.degreecount;

import java.util.List;

import mapreduce.Outputer;
import mapreduce.Reducer;

public class DegreeCountReducer extends Reducer {

  @Override
  public void reduce(String key, List<String> values, Outputer out) {
    int indegree = 0;
    int outdegree = 0;
    for (String s : values) {
      if (s.equals("in"))
        indegree++;
      else
        outdegree++;
    }
    out.collect(key, String.valueOf(indegree) + "\t" + String.valueOf(outdegree));
  }
}
