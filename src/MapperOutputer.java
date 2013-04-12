import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class MapperOutputer extends Outputer {
  
  private Partitioner partitioner;

  private BufferedWriter[] writers;
  
  //private ArrayList<Record>[] buffer;

  public MapperOutputer(String dir, Partitioner partitioner) {
    super(dir);
    this.partitioner = partitioner;
    this.writers = new BufferedWriter[this.partitioner.getReducerNum()];
    for (int i = 0; i < this.writers.length; i++) {
      try {
        writers[i] = new BufferedWriter(new FileWriter(this.outputDir
                + System.getProperty("file.separator") + MapperOutputer.defaultName + i, true));
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

  @Override
  public void collect(String key, String value) {
    int part = this.partitioner.getPartition(key);
    
    BufferedWriter bw = this.writers[part];
    try {
      bw.write(key + MapperOutputer.separator + value);
      bw.newLine();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
