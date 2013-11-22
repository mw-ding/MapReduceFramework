package example.wordcount;

import java.io.IOException;

import mapreduce.InputFormat;
import mapreduce.Record;

public class WCInputFormat extends InputFormat {
  
  public WCInputFormat(String filename, Long offset, Integer blockSize) throws IOException {
    super(filename, offset, blockSize);
  }

  @Override
  public boolean hasNext() {
    /* check if the block ends or not */
    try {
      return this.hasByte();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public Record next() {
    try {
      /* read next line */
      String line = this.raf.readLine();
      
      String key = Integer.toString(line.length());
      String value = line;
      /* return a record built with the key and value */
      return new Record(key, value);
      // return new Record(line,line);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void remove() {

  }
}
