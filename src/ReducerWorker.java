import java.util.*;
import java.util.Map.Entry;


public class ReducerWorker extends Worker {

  private Output outputer;
  
  private Reducer reducer;

  private float copyPercentage;
  
  private float groupPercentage;
  
  private float reducePercentage;
  
  public ReducerWorker(int taskID, String inputFile, int offset, int blockSize, String outputFile,
          String code, String taskTrackerServiceName) {
    super(taskID, outputFile, taskTrackerServiceName);
    
    this.copyPercentage = 0;
    this.groupPercentage = 0;
    this.reducePercentage = 0;
  }
  
  private List<Record> copy() {
    
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
    this.outputer.closeAll();
    
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
