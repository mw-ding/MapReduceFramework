public class MapperWorker extends Worker {

  private String inputFile;

  private int offset;

  private int blockSize;

  private int reducerNum;
  
  private Partitioner partitioner;
  
  private Mapper mapper;

  public MapperWorker(int taskID, String inputFile, int offset, int blockSize, String outputFile,
          String code, int numReducer, String taskTrackerServiceName) {
    super(taskID, outputFile, code, taskTrackerServiceName);
    this.inputFile = inputFile;
    this.offset = offset;
    this.blockSize = blockSize;
    this.reducerNum = numReducer;
    this.partitioner = new Partitioner(this.reducerNum);
    try {
      this.mapper = (Mapper)Class.forName(this.code).newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    System.out.println("MAPPER JOB WILL RUN");
    mapper.setup();
  }

  public float getPercentage() {
    return 0;
  }

  public static void main(String[] args) {
    if (args.length != 8) {
      System.out.println("Illegal arguments");
    }
    int taskID = Integer.parseInt(args[0]);
    String inputFile = args[1];
    int offset = Integer.parseInt(args[2]);
    int blockSize = Integer.parseInt(args[3]);
    String outputFile = args[4];
    String code = args[5];
    int reducerNum = Integer.parseInt(args[6]);
    String taskTrackerServiceName = args[7];
    MapperWorker worker = new MapperWorker(taskID, inputFile, offset, blockSize, outputFile, code,
            reducerNum, taskTrackerServiceName);
    worker.run();
  }

}
