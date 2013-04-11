import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.rmi.RemoteException;

/* TODO: the input format should be assigned by user, here it is fixed */
public class MapperWorker extends Worker {

  private String inputFile;

  private long offset;

  private int blockSize;

  private int reducerNum;

  private Output output;

  private Mapper mapper;

  private InputFormat inputFormat;

  public MapperWorker(int taskID, String inputFile, long offset, int blockSize, String outputFile,
          String mapper, String partitioner, String inputFormat, int numReducer,
          String taskTrackerServiceName) {
    super(taskID, outputFile, taskTrackerServiceName);
    this.inputFile = inputFile;
    this.offset = offset;
    this.blockSize = blockSize;
    this.reducerNum = numReducer;
    try {
      /* initialize the mapper */
      this.mapper = (Mapper) Class.forName(mapper).newInstance();
      /* initialize the output with user-defined or default partitioner */
      Partitioner part = (Partitioner) Class.forName(partitioner).getConstructor(Integer.class)
              .newInstance(this.reducerNum);
      this.output = new Output(this.inputFile, part);
      /* initialize the user-defined or default input format */
      this.inputFormat = (InputFormat) Class.forName(inputFormat)
              .getConstructor(String.class, Long.class, Integer.class)
              .newInstance(this.inputFile, this.offset, this.blockSize);
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
      mapper.map(record.key, record.value, this.output);
    }
    /* close the files */
    this.output.closeAll();
    /* do cleanup */
    mapper.cleanup();
    /* report to task tracker that this task is done */
    this.updateStatusSucceed();
    System.exit(0);
  }

  public float getPercentage() {
    try {
      return (float) (offset - this.inputFormat.raf.getFilePointer()) / this.blockSize;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return 0;
  }

  public static void main(String[] args) {
    if (args.length != 10) {
      System.out.println("Illegal arguments");
    }
    int taskID = Integer.parseInt(args[0]);
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
