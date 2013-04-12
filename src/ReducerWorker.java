import java.io.*;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.Map.Entry;

public class ReducerWorker extends Worker {

  private int orderId;

  private ReducerOutputer outputer;

  private OutputFormat formater;

  private Reducer reducer;

  private MapStatusChecker mapStatusChecker;

  private float copyPercentage;

  private float groupPercentage;

  private float reducePercentage;

  private static int SLEEP_CIRCLE = 100;

  public ReducerWorker(int taskID, int order, String reducer, String oformater, String indir,
          String outdir, String taskTrackerServiceName) {
    super(taskID, indir, outdir, taskTrackerServiceName, TaskType.REDUCER);

    File outdirfile = new File(this.outputFile);
    outdirfile.mkdir();

    this.orderId = order;
    try {
      this.reducer = (Reducer) Class.forName(reducer).newInstance();
      this.formater = (OutputFormat) Class.forName(oformater).newInstance();
      this.outputer = new ReducerOutputer(this.outputFile + File.separator + Outputer.defaultName
              + this.orderId, this.formater);
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    /* get the task tracker status updater */
    String registryHostName = Utility.getParam("REGISTRY_HOST");
    int registryPort = Integer.parseInt(Utility.getParam("REGISTRY_PORT"));

    try {
      Registry reg = LocateRegistry.getRegistry(registryHostName, registryPort);
      mapStatusChecker = (MapStatusChecker) reg
              .lookup(Utility.getParam("JOB_TRACKER_SERVICE_NAME"));
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (NotBoundException e) {
      e.printStackTrace();
    }

    this.copyPercentage = 0;
    this.groupPercentage = 0;
    this.reducePercentage = 0;
  }

  /**
   * locate all mapper's output files for this reducer
   * 
   * @return
   */
  private List<File> locateMapOutput() {
    while (!mapStatusChecker.isAllMapperFinished(this.taskID)) {
      try {
        Thread.sleep(this.SLEEP_CIRCLE);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    List<File> result = new ArrayList<File>();

    File indirfile = new File(this.inputFile);
    if (!indirfile.isDirectory()) {
      System.out.println("Invalid Reducer input dir.");
      return result;
    }

    File[] mapOutputDirs = indirfile.listFiles();
    String filename = Outputer.defaultName + this.orderId;
    for (File mapOutputDir : mapOutputDirs) {
      result.add(new File(mapOutputDir.getAbsolutePath() + File.separator + filename));
    }

    return result;
  }

  /**
   * read and parse all key/value pairs from mapper's output files
   * 
   * @return
   */
  private List<Record> copy() {
    List<Record> result = new ArrayList<Record>();
    List<File> mapOutput = this.locateMapOutput();

    final float csize = mapOutput.size();
    float cfinished = (float) 0.0;

    for (File f : mapOutput) {
      try {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));

        String line = null;
        while ((line = reader.readLine()) != null) {
          String[] fields = line.split(Outputer.separator);
          result.add(new Record(fields[0], fields[1]));
        }

      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

      cfinished += (float) 1.0;
      this.copyPercentage = cfinished / csize;
    }

    this.copyPercentage = (float) 1.0;
    return null;
  }

  private Map<String, List<String>> group(List<Record> records) {
    // use tree map to make sure that the all the result are sorted by key
    Map<String, List<String>> result = new TreeMap<String, List<String>>();

    final float lsize = records.size();
    float lfinished = (float) 0.0;

    for (Record record : records) {
      String key = record.key;

      if (!result.containsKey(key)) {
        result.put(key, new ArrayList<String>());
      }

      result.get(key).add(record.value);

      lfinished += (float) 1.0;
      this.groupPercentage = lfinished / lsize;
    }

    this.groupPercentage = (float) 1.0;
    return result;
  }

  @Override
  public void run() {
    // periodically update status to task tracker
    updateStatusToTaskTracker();

    // 1. do setup
    this.reducer.setup();

    // 2. copy key/value pairs from mappers
    List<Record> unsorted = this.copy();

    // 3. sort and group all key/value pairs
    Map<String, List<String>> grouped = this.group(unsorted);
    this.groupPercentage = (float) 1.0;

    // 4. execute the reduce function
    final float gsize = grouped.size();
    float gfinished = (float) 0.0;
    for (Entry<String, List<String>> entry : grouped.entrySet()) {
      this.reducer.reduce(entry.getKey(), entry.getValue(), this.outputer);

      gfinished += 1.0;
      this.reducePercentage = gfinished / gsize;
    }
    this.reducePercentage = (float) 1.0;

    // 5. close the outputer
    this.outputer.close();

    // 6. do cleanup
    this.reducer.cleanup();

    // report accomplishment to task tracker
    this.updateStatusSucceed();
    System.exit(0);
  }

  @Override
  public float getPercentage() {
    return (this.copyPercentage + this.groupPercentage + this.reducePercentage) / (float) 3.0;
  }

  public static void main(String[] args) {
    if (args.length != 7) {
      System.out.println("Illegal arguments");
    }
    int taskID = Integer.parseInt(args[0]);
    int order = Integer.parseInt(args[1]);
    String reducer = args[2];
    String outputFormat = args[3];
    String indir = args[4];
    String outdir = args[5];
    String taskTrackerServiceName = args[6];
    ReducerWorker worker = new ReducerWorker(taskID, order, reducer, outputFormat, indir, outdir,
            taskTrackerServiceName);
    worker.run();
  }

}
