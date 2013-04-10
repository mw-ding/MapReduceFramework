public class MapperWorker extends Worker {

  String inputFile;

  int offset;

  int blockSize;

  int numReducer;

  public MapperWorker(int taskID, String inputFile, int offset, int blockSize, String outputFile,
          String code, int numReducer, String taskTrackerServiceName) {
    super(taskID, outputFile, code, taskTrackerServiceName);
    this.inputFile = inputFile;
    this.offset = offset;
    this.blockSize = blockSize;
    this.numReducer = numReducer;
  }

  @Override
  public void run() {
    System.out.println("MAPPER JOB WILL RUN");
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
