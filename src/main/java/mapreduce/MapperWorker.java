package mapreduce;

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

public class MapperWorker extends Worker {

  /* the block offset */
  private long offset;

  /* the block size */
  private int blockSize;

  /* number of reducer, needed to partition */
  private int reducerNum;

  /* how to output the mapper result */
  private MapperOutputer outputer;

  /* the mapper class */
  private Mapper mapper;

  /* how to parse the input file to key-value pair */
  private InputFormat inputFormat;

  /**
   * constructor method
   * 
   * @param taskID
   * @param infile
   * @param offset
   * @param blockSize
   * @param outfile
   * @param mapper
   * @param partitioner
   * @param inputFormat
   * @param numReducer
   * @param taskTrackerServiceName
   *          : where to contact the task tracker
   */
  public MapperWorker(int taskID, String infile, long offset, int blockSize, String outfile,
          String mapper, String partitioner, String inputFormat, int numReducer,
          String taskTrackerServiceName, int rPort) {
    super(taskID, infile, outfile, taskTrackerServiceName, TaskMeta.TaskType.MAPPER, rPort);
    this.offset = offset;
    this.blockSize = blockSize;
    this.reducerNum = numReducer;

    try {
      /* initialize the mapper */
      this.mapper = (Mapper) Class.forName(mapper).newInstance();

      /* initialize the output with user-defined or default partitioner */
      Partitioner part = (Partitioner) Class.forName(partitioner).getConstructor(Integer.class)
              .newInstance(new Integer(this.reducerNum));

      this.outputer = new MapperOutputer(this.outputFile, part);

      /* initialize the user-defined or default input format */
      this.inputFormat = (InputFormat) Class.forName(inputFormat)
              .getConstructor(String.class, Long.class, Integer.class)
              .newInstance(this.inputFile, new Long(this.offset), new Integer(this.blockSize));
    } catch (Exception e) {
      e.printStackTrace();
      /* exception happens, shut down jvm */
      System.exit(0);
    }
    System.out.println("contructor done");
  }

  @Override
  public void run() {
    /* periodically update status to task tracker */
    updateStatusToTaskTracker();
    /* do setup */
    mapper.setup();
    /* do map */
    try {
      while (this.inputFormat.hasNext()) {
        Record record = this.inputFormat.next();
        mapper.map(record.key, record.value, this.outputer);
      }
      /* close the files */
      this.outputer.closeAll();
    }/* if runtime exception happens in user's code, exit jvm */
    catch (RuntimeException e) {
      e.printStackTrace();
      System.exit(0);
    }
    /* sort the files */
    this.sort();
    /* do cleanup */
    mapper.cleanup();
    /* report to task tracker that this task is done */
    this.updateStatusSucceed();
    /* shut down jvm */
    System.exit(0);
  }

  /**
   * @return percentage of work already done
   */
  protected float getPercentage() {
    try {
      /* the percentage of work is: bytes already processed / block size */
      return (float) (this.inputFormat.raf.getFilePointer() - offset) / this.blockSize;
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(0);
    }
    return 0;
  }

  /**
   * in-memory sort the temp files produced by mappers
   */
  private void sort() {
    /* for each partition */
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
            System.exit(0);
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
            System.exit(0);
          }
      }
    }
  }

  public static void main(String[] args) {
    if (args.length != 11) {
      System.out.println("Illegal arguments");
    }
    int taskID = Integer.parseInt(args[0]);
    try {
      PrintStream out = new PrintStream(new FileOutputStream(new File(
              Utility.getParam("MAPPER_STANDARD_OUT_REDIRECT") + taskID)));
      PrintStream err = new PrintStream(new FileOutputStream(new File(
              Utility.getParam("MAPPER_STANDARD_ERR_REDIRECT") + taskID)));
      System.setErr(err);
      System.setOut(out);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      System.exit(0);
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
    int rPort = Integer.parseInt(args[10]);
    MapperWorker worker = new MapperWorker(taskID, inputFile, offset, blockSize, outputFile,
            mapper, partitioner, inputFormat, reducerNum, taskTrackerServiceName, rPort);
    worker.run();
  }

}
