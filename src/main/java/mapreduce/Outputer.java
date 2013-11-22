package mapreduce;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public abstract class Outputer {

  public static String defaultName = "part-";

  public static String separator = "\t";

  protected String outputDir;
  
  public Outputer(String dir) {
    this.outputDir = dir;
  }

  public abstract void collect(String key, String value);
}
