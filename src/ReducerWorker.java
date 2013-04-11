import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class ReducerWorker extends Worker {
  
  private int taskId;
  
  private int orderId;

  private ReducerOutputer outputer;
  
  private OutputFormat formater;

  private Reducer reducer;

  private float copyPercentage;

  private float groupPercentage;

  private float reducePercentage;

  public ReducerWorker(int taskID, int order, String reducer, String oformater, String indir, String outdir,
          String taskTrackerServiceName) {
    super(taskID, indir, outdir, taskTrackerServiceName);
    
    File outdirfile = new File(this.outputFile);
    outdirfile.mkdir();
    
    this.taskId = taskID;
    this.orderId = order;
    try {
      this.reducer = (Reducer) Class.forName(reducer).newInstance();
      this.formater = (OutputFormat) Class.forName(oformater).newInstance();
      this.outputer = new ReducerOutputer(this.outputFile + File.separator + Outputer.defaultName + this.orderId, this.formater);
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    
    this.copyPercentage = 0;
    this.groupPercentage = 0;
    this.reducePercentage = 0;
  }
  
  /**
   * locate all mapper's output files for this reducer
   * @return
   */
  private List<File> locateMapOutput() {
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
        while( (line = reader.readLine()) != null ) {
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

}
