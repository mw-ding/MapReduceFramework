package mapreduce;

import java.io.*;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.Map.Entry;

public class ReducerWorker extends Worker {
  
  // the stream for merge sort in reducer
  private class MergeStream {
    
    private Record next;
    
    private Scanner scanner;
    
    public MergeStream(Scanner s) {
      this.scanner = s;
      this.next = null;
    }
    
    public void tryFetchNext() {
      if (this.scanner.hasNext()) {
        String line = this.scanner.nextLine();
        String[] fields = line.split("\t");
        this.next = new Record(fields[0], fields[1]);
      } else {
        this.next = null;
      }
    }
    
    public Record getNext() {
      return this.next;
    }
  }
  
  // the key/value iterator for reduce()
  private class KeyValueIterator implements Iterator<String> {
    
    private PriorityQueue<MergeStream> streams;
    
    private String curKey;
    
    public KeyValueIterator(List<File> flist) {
      if (flist == null)
        return ;
      
      // initializ the heap
      int size = flist.size();
      this.streams = new PriorityQueue<MergeStream>(size, new Comparator<MergeStream>() {

        @Override
        public int compare(MergeStream stream1, MergeStream stream2) {
          return stream1.getNext().key.compareTo(stream2.getNext().key);
        }
        
      });
      
      for (File f : flist) {
        try {
          Scanner newscanner = new Scanner(new FileInputStream(f));
          MergeStream newstream = new MergeStream(newscanner);
          
          // try to fetch the first line
          newstream.tryFetchNext();
          
          // if failed to fetch the first line, do not add
          // it into the queue; otherwise, add it in.
          if (newstream.getNext() != null) {
            this.streams.add(newstream);
          }
          
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        }
      }
      
      this.curKey = null;
    }

    @Override
    public boolean hasNext() {
      return (!this.streams.isEmpty() && this.streams.peek().getNext().key.compareTo(this.curKey) == 0);
    }

    @Override
    public String next() {
      if (!this.streams.isEmpty()) {
        MergeStream curstream = this.streams.poll();
        String result = curstream.getNext().value;

        // try to fetch next record from this stream;
        // it the next record exists, insert this stream back
        // to the heap; otherwise, drop this stream, because
        // it reaches the end
        curstream.tryFetchNext();
        if (curstream.getNext() != null) {
          this.streams.add(curstream);
        }

        return result;
      } else {
        return null;
      }
    }

    @Override
    public void remove() {
      // do not support remove operation
    }
    
    public String currentKey() {
      return this.streams.peek().getNext().key;
    }
    
    public boolean continueNextKey() {
      if (this.streams.isEmpty()) return false;
      else {
        // set curKey to nextKey to allow next iteration
        this.curKey = this.currentKey();
        return true;
      }
    }
  }

  private int orderId;

  private ReducerOutputer outputer;

  private OutputFormat formater;

  private Reducer reducer;

  private MapStatusChecker mapStatusChecker;

  private float copyPercentage;

  private float groupPercentage;

  private float reducePercentage;

  private final int SLEEP_CYCLE;

  public ReducerWorker(int taskID, int order, String reducer, String oformater, String indir,
          String outdir, String taskTrackerServiceName) {
    super(taskID, indir, outdir, taskTrackerServiceName, TaskMeta.TaskType.REDUCER);

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
    SLEEP_CYCLE = Integer.parseInt(Utility.getParam("REDUCER_CHECK_MAPPER_CYCLE"));
  }

  /**
   * locate all mapper's output files for this reducer
   * 
   * @return
   */
  private List<File> locateMapOutput() {
    try {
      MapStatusChecker.MapStatus res = mapStatusChecker.checkMapStatus(this.taskID);
      while (res != MapStatusChecker.MapStatus.FINISHED) {

        if (res == MapStatusChecker.MapStatus.FAILED) {
          System.exit(0);
        }

        try {
          Thread.sleep(this.SLEEP_CYCLE);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        res = mapStatusChecker.checkMapStatus(this.taskID);
      }
    } catch (RemoteException e) {
      e.printStackTrace();
    }
    List<File> result = new ArrayList<File>();

    File indirfile = new File(this.inputFile);
    if (!indirfile.isDirectory()) {
      System.err.println("Invalid Reducer input dir.");
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
  private List<File> copy() {
    // in current scenario, all we need to do in copy phase is
    // locating the temporary output files from mappers, which
    // could be done almost at once
    List<File> result = this.locateMapOutput();
    
    this.copyPercentage = (float) 1.0;
    
    return result;
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
    List<File> mapperOutputFiles = this.copy();

    // 3. sort and group all key/value pairs
    this.groupPercentage = (float) 1.0;
    
    // 4. call the reduce and feed it with key/value pairs
    KeyValueIterator kviterator = new KeyValueIterator(mapperOutputFiles);
    while(kviterator.continueNextKey()) {
      String key = kviterator.currentKey();
      System.out.println("reduce framework key : " + key );
      try {
        this.reducer.reduce(key, kviterator, this.outputer);
      } catch (RuntimeException e) {
        e.printStackTrace();
        System.exit(0);
      }
      
      // skip to next key, if the user does not fetch all values
      while(kviterator.hasNext()) {
        kviterator.next();
      }
    }
    
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

    // for test
    try {
      PrintStream out = new PrintStream(new FileOutputStream(new File(
              Utility.getParam("REDUCER_STANDARD_OUT_REDIRECT") + taskID)));
      PrintStream err = new PrintStream(new FileOutputStream(new File(
              Utility.getParam("REDUCER_STANDARD_ERR_REDIRECT") + taskID)));
      System.setErr(err);
      System.setOut(out);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

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
