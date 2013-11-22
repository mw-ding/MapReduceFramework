package mapreduce;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * how the result of mapper is output to different partition file
 * 
 */
public class MapperOutputer extends Outputer {
  
  private Partitioner partitioner;

  /* the writers for different partition */
  private BufferedWriter[] writers;

  public MapperOutputer(String dir, Partitioner partitioner) {
    super(dir);
    this.partitioner = partitioner;
    this.writers = new BufferedWriter[this.partitioner.getReducerNum()];
    for (int i = 0; i < this.writers.length; i++) {
      try {
        /* build writers for different partitions */
        writers[i] = new BufferedWriter(new FileWriter(this.outputDir
                + System.getProperty("file.separator") + MapperOutputer.defaultName + i, true));
      } catch (IOException e) {
        System.err.println("Output failed to create files");
      }
    }
  }
  
  /**
   * After the writes are done, close all files
   */
  public void closeAll() {
    for (BufferedWriter bw : writers) {
      try {
        bw.flush();
        bw.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  /**
   * write to different partitions
   */
  @Override
  public void collect(String key, String value) {
    if (this.partitioner == null)
      System.out.println("partitioner null");
    int part = this.partitioner.getPartition(key);
    
    BufferedWriter bw = this.writers[part];
    try {
      bw.write(key + MapperOutputer.separator + value);
      bw.newLine();
      bw.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
