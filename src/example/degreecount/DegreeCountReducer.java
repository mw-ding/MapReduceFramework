package example.degreecount;

import java.util.Iterator;
import java.util.List;

import mapreduce.Outputer;
import mapreduce.Reducer;

public class DegreeCountReducer extends Reducer {

  @Override
  public void reduce(String key, Iterator<String> values, Outputer out) {
    int indegree = 0;
    int outdegree = 0;
    while(values.hasNext()){
      String s = values.next();
      if (s.equals("in"))
        indegree++;
      else
        outdegree++;
    }
    out.collect(key, String.valueOf(indegree) + "\t" + String.valueOf(outdegree));
  }
}
