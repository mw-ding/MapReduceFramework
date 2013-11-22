package mapreduce;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;

public abstract class InputFormat implements Iterator<Record> {
  
  /* this file name */
  protected String filename;

  /* the offset of the block */
  protected long offset;

  /* the block size */
  protected int blockSize;

  protected RandomAccessFile raf;
  
  /**
   * constructor method
   * @param filename
   * @param offset
   * @param blockSize
   * @throws IOException
   */
  protected InputFormat(String filename, Long offset, Integer blockSize) throws IOException {
    this.filename = filename;
    this.offset = offset;
    this.blockSize = blockSize;
    this.raf = new RandomAccessFile(filename, "r");
    if(offset != 0){
      /* check if offset is start of line*/
      this.raf.seek(offset-1);
      if((char)this.raf.readByte() != '\n'){/* offset is not start of line */
        this.raf.readLine();/* jump to next line */
      }else{/* if offset is the start of line */
        this.raf.seek(offset);
      }
    }else/* if offset is the start of file */
      this.raf.seek(offset);
  }

  /**
   *  check if there are still bytes in this block 
   *  */
  protected boolean hasByte() throws IOException {
    if (this.raf.getFilePointer() < (this.offset + this.blockSize))
      return true;
    return false;
  }
}
