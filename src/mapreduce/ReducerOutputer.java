package mapreduce;
import java.io.*;


public class ReducerOutputer extends Outputer {
  
  private OutputFormat formator;
  
  private BufferedWriter writer;

  public ReducerOutputer(String dir, OutputFormat of) {
    super(dir);
    
    this.formator = of;
    try {
      this.writer = new BufferedWriter(new FileWriter(this.outputDir));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public void close() {
    if (this.writer != null) {
      try {
        this.writer.flush();
        this.writer.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void collect(String key, String value) {
    try {
      this.writer.write(this.formator.format(key, value));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
