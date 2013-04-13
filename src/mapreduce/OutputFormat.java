package mapreduce;

/* the format of output by reducer */
public abstract class OutputFormat {

  public abstract String format(String key, String value);
  
}
