public class MapperWorker extends Worker {

  String inputFile;

  int offset;

  int blockSize;

  public MapperWorker(int taskID, String inputFile, int offset, int blockSize, String outputFile,
          String code, String taskTrackerServiceName) {
    super(taskID, outputFile, code, taskTrackerServiceName);
    this.inputFile = inputFile;
    this.offset = offset;
    this.blockSize = blockSize;
  }

  @Override
  public void run() {
    System.out.println("MAPPER JOB WILL RUN");
  }

  public float getPercentage() {
    return 0;
  }

  public static void main(String[] args) {
    if (args.length != 7) {
      System.out.println("Illegal arguments");
    }
    int taskID = Integer.parseInt(args[0]);
    String inputFile = args[1];
    int offset = Integer.parseInt(args[2]);
    int blockSize = Integer.parseInt(args[3]);
    String outputFile = args[4];
    String code = args[5];
    String taskTrackerServiceName = args[6];
    MapperWorker worker = new MapperWorker(taskID, inputFile, offset, blockSize, outputFile, code,
            taskTrackerServiceName);
    worker.run();
  }

}
