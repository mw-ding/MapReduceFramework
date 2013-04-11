import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Output {

  public static String defaultName = "part-";

  public static String separator = "\t";

  public String outputDir;

  private Partitioner partitioner;

  private BufferedWriter[] writers;

  public Output(String dir, Partitioner partitioner) {
    this.outputDir = dir;
    this.partitioner = partitioner;
    for (int i = 0; i < this.partitioner.getReducerNum(); i++) {
      try {
        writers[i] = new BufferedWriter(new FileWriter(this.outputDir
                + System.getProperty("file.separator") + Output.defaultName + i, true));
      } catch (IOException e) {
        System.err.println("Output failed to create files");
      }
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
    int part = this.partitioner.getPartition(key);
    BufferedWriter bw = this.writers[part];
    try {
      bw.write(key + Output.separator + value);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
