import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Output {

  public static String defaultName = "part-";

  public static String separator = "\t";

  public String outputDir;

  private Partitioner partitioner;

  private BufferedWriter[] writers;

  /**
   * Constructor for Mapper, which needs partitioner
   * @param dir
   * @param partitioner
   */
  public Output(String dir, Partitioner partitioner) {
    this.outputDir = dir;
    this.partitioner = partitioner;
    this.writers = new BufferedWriter[this.partitioner.getReducerNum()];
    for (int i = 0; i < this.writers.length; i++) {
      try {
        writers[i] = new BufferedWriter(new FileWriter(this.outputDir
                + System.getProperty("file.separator") + Output.defaultName + i, true));
      } catch (IOException e) {
        System.err.println("Output failed to create files");
      }
    }
  }

  /**
   * Constructor for Reducer
   * @param filepath
   */
  public Output(String filepath) {
    this.outputDir = filepath;
    this.partitioner = null;
    this.writers = new BufferedWriter[1];
    try {
      this.writers[0] = new BufferedWriter(new FileWriter(this.outputDir));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void closeAll() {
    for (BufferedWriter bw : writers) {
      try {
        bw.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void write(String key, String value) {
    int part = 0;
    if (this.partitioner != null)
      part = this.partitioner.getPartition(key);
    
    BufferedWriter bw = this.writers[part];
    try {
      bw.write(key + Output.separator + value);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
