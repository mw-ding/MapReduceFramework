package example.io;

import mapreduce.OutputFormat;

public class MyOutputFormat extends OutputFormat {

  @Override
  public String format(String key, String value) {
    return key + "\t" + value + "\n";
  }

}
