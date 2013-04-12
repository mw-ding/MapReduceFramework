import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;

/* TODO: the input format should be assigned by user, here it is fixed */
public class MapperWorker extends Worker {

  private long offset;

  private int blockSize;

  private int reducerNum;

  private MapperOutputer outputer;

  private Mapper mapper;

  private InputFormat inputFormat;

  public MapperWorker(int taskID, String infile, long offset, int blockSize, String outfile,
          String mapper, String partitioner, String inputFormat, int numReducer,
          String taskTrackerServiceName) {
    super(taskID, infile, outfile, taskTrackerServiceName, TaskType.MAPPER);
    this.offset = offset;
    this.blockSize = blockSize;
    this.reducerNum = numReducer;
    
    try {
      /* initialize the mapper */
      this.mapper = (Mapper) Class.forName(mapper).newInstance();
      
      /* initialize the output with user-defined or default partitioner */
      Partitioner part = (Partitioner) Class.forName(partitioner).getConstructor(Integer.class)
              .newInstance(new Integer(this.reducerNum));
      System.out.println(part.getReducerNum());
      
      this.outputer = new MapperOutputer(this.outputFile, part);
      
      /* initialize the user-defined or default input format */
      this.inputFormat = (InputFormat) Class.forName(inputFormat)
              .getConstructor(String.class, Long.class, Integer.class)
              .newInstance(this.inputFile, new Long(this.offset), new Integer(this.blockSize));   
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (SecurityException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    /* periodically update status to task tracker */
    updateStatusToTaskTracker();
    /* do setup */
    mapper.setup();
    /* do map */
    while (this.inputFormat.hasNext()) {
      Record record = this.inputFormat.next();
      mapper.map(record.key, record.value, this.outputer);
    }
    /* close the files */
    this.outputer.closeAll();
    /* do cleanup */
    mapper.cleanup();
    /* report to task tracker that this task is done */
    this.updateStatusSucceed();
    System.exit(0);
  }

  protected float getPercentage() {
    try {
      return (float) (offset - this.inputFormat.raf.getFilePointer()) / this.blockSize;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return 0;
  }

  private void sort() {
    for (int i = 0; i < this.reducerNum; i++) {
      ArrayList<Record> list = new ArrayList<Record>();
      String filename = outputer.outputDir + System.getProperty("file.separator")
              + Outputer.defaultName + i;
      File file = new File(filename);
      /* read file for each partition, wrap to records, store to list */
      BufferedReader br = null;
      try {
        br = new BufferedReader(new FileReader(file));
        String line;
        while ((line = br.readLine()) != null) {
          int ind = line.indexOf('\t');
          String key = line.substring(0, ind);
          String value = line.substring(ind + 1);
          Record record = new Record(key, value);
          list.add(record);
        }
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        if (br != null)
          try {
            br.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
      }
      /* delete the file */
      file.delete();
      /* sort the records */
      Collections.sort(list);
      /* write the sorted records to file */
      BufferedWriter bw = null;
      try {
        bw = new BufferedWriter(new FileWriter(file, true));
        for (Record r : list) {
          bw.write(r.key + Outputer.separator + r.value);
          bw.newLine();
        }
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        if (bw != null)
          try {
            bw.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
      }
    }
  }

  public static void main(String[] args) {    
    if (args.length != 10) {
      System.out.println("Illegal arguments");
    }
    int taskID = Integer.parseInt(args[0]);
    
    // for test
    try {
      PrintStream out = new PrintStream(new FileOutputStream(new File("/Users/dmw1989/Documents/workspace/MapReduceFramework/mapout" + taskID)));
      PrintStream err = new PrintStream(new FileOutputStream(new File("/Users/dmw1989/Documents/workspace/MapReduceFramework/maperr" + taskID)));
      System.setErr(err);
      System.setOut(out);
      System.out.println(System.getProperty("java.class.path"));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    
    String inputFile = args[1];
    long offset = Long.parseLong(args[2]);
    int blockSize = Integer.parseInt(args[3]);
    String outputFile = args[4];
    String mapper = args[5];
    String partitioner = args[6];
    String inputFormat = args[7];
    int reducerNum = Integer.parseInt(args[8]);
    String taskTrackerServiceName = args[9];
    MapperWorker worker = new MapperWorker(taskID, inputFile, offset, blockSize, outputFile,
            mapper, partitioner, inputFormat, reducerNum, taskTrackerServiceName);
    worker.run();
  }

}
