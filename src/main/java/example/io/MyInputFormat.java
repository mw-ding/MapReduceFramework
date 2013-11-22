package example.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;

import mapreduce.InputFormat;
import mapreduce.Record;

public class MyInputFormat extends InputFormat {

  public MyInputFormat(String filename, Long offset, Integer blockSize) throws IOException {
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
      /* split the current line into key and value by tab */
      int tabInd = line.indexOf('\t');
      String key = line.substring(0, tabInd);
      String value = line.substring(tabInd + 1);
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
