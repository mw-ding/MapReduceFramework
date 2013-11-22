package mapreduce;
import java.io.File;
import java.util.*;

public class JobMeta {

  public enum JobStatus {
    INIT, INPROGRESS, FAILED, SUCCEED
  }

  // the input blocks after splitting the input data
  public class InputBlock {
    private String filePath;

    private long offset;

    private int length;

    public InputBlock(String fp, long o, int l) {
      this.filePath = fp;
      this.offset = o;
      this.length = l;
    }

    public String getFilePath() {
      return filePath;
    }

    public long getOffset() {
      return offset;
    }

    public int getLength() {
      return length;
    }
  }

  // the id for the job
  private int jobId;

  // the name for each map/reduce job
  private String jobName;

  // the class name of the mapper
  private String mapperClassName;

  // the class name of the reducer
  private String reducerClassName;
  
  // the class name of the partitioner
  private String partitionerClassName;
  
  // the class name of the input formatter
  private String inputFormatClassName;
  
  // the class name of the output formatter
  private String outputFormatClassName;

  // the input path of this job
  private String inputPath;

  // the output path of this job
  private String outputPath;

  // the input block size
  private int blockSize;

  // the number of reducers
  private int reducerNum;

  // the ids of all mappers in this job
  private Set<Integer> mapTasks;

  // the ids of all reducers in this job
  private Set<Integer> reduceTasks;
  
  // the number of tasks that finished
  private int taskFinishedMapperNum;
  
  // the number of tasks that finished
  private int taskFinishedReducerNum;

  // the input blocks
  private List<InputBlock> inputBlocks;
  
  // the status of this job
  private JobStatus status;

  public JobMeta(JobConf jconf) {
    this.jobId = jconf.getJobID();
    this.jobName = jconf.getJobName();
    this.mapperClassName = jconf.getMapperClassName();
    this.reducerClassName = jconf.getReducerClassName();
    this.partitionerClassName = jconf.getPartitionerClassName();
    this.inputFormatClassName = jconf.getInputFormatClassName();
    this.outputFormatClassName = jconf.getOutputFormatClassName();
    this.inputPath = jconf.getInputPath();
    this.outputPath = jconf.getOutputPath();
    this.blockSize = jconf.getBlockSize();
    this.reducerNum = jconf.getReducerNum();
    this.taskFinishedMapperNum = 0;
    this.taskFinishedReducerNum = 0;
    this.status = JobStatus.INIT;

    this.mapTasks = new HashSet<Integer>();
    this.reduceTasks = new HashSet<Integer>();
    this.inputBlocks = new ArrayList<InputBlock>();
  }

  /**
   * split all the input files
   */
  public void splitInput() {
    if (this.inputBlocks == null)
      this.inputBlocks = new ArrayList<InputBlock>();
    else
      this.inputBlocks.clear();

    File f = new File(this.inputPath);
    if (f.isDirectory()) {
      // here, we assume that there could only be one level directory for
      // the input directory
      File[] files = f.listFiles();
      for (File file : files)
        this.inputBlocks.addAll(this.splitOneInputFile(file));
    } else {
      this.inputBlocks.addAll(this.splitOneInputFile(f));
    }
  }

  /**
   * split a single file
   * 
   * @param file
   * @return
   */
  private List<InputBlock> splitOneInputFile(File file) {
    List<InputBlock> result = new ArrayList<InputBlock>();

    long len = file.length();
    long offset = 0;
    while (len > 0) {
      if (len > (this.blockSize + this.blockSize / 2)) {
        // if the rest content is 1.5 times long than the blocksize, just split
        // a normal block
        result.add(new InputBlock(file.getAbsolutePath(), offset, this.blockSize));
        len -= this.blockSize;
        offset += this.blockSize;
      } else {
        // otherwise, take all the rest content as a block
        result.add(new InputBlock(file.getAbsolutePath(), offset, (int) len));
        len = 0;
      }
    }

    return result;
  }
  
  /**
   * check whether this job is done
   * @return
   */
  public boolean isDone() {
    return ((this.taskFinishedMapperNum + this.taskFinishedReducerNum) == (this.mapTasks.size() + this.reduceTasks.size()));
  }
  
  /**
   * update the finished task status
   * @param tid
   */
  public void reportFinishedTask(int tid) {
    if (this.mapTasks.contains(tid)) {
      this.taskFinishedMapperNum ++;
    }
    
    if (this.reduceTasks.contains(tid)) {
      this.taskFinishedReducerNum ++;
    }
  }
  
  public void addMapperTask(int taskid) {
    this.mapTasks.add(taskid);
  }

  public void addReducerTask(int taskid) {
    this.reduceTasks.add(taskid);
  }
  
  // the following are the getters of all fields
  public int getFinishedMapperNumber() {
    return this.taskFinishedMapperNum;
  }
  
  public int getFinishedReducerNumber() {
    return this.taskFinishedReducerNum;
  }

  public int getJobId() {
    return jobId;
  }
  
  public String getJobName() {
    return jobName;
  }

  public String getMapperClassName() {
    return mapperClassName;
  }

  public String getReducerClassName() {
    return reducerClassName;
  }

  public String getInputPath() {
    return inputPath;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public Set<Integer> getMapTasks() {
    return mapTasks;
  }

  public Set<Integer> getReduceTasks() {
    return reduceTasks;
  }

  public List<InputBlock> getInputBlocks() {
    return Collections.unmodifiableList(inputBlocks);
  }

  public int getReducerNum() {
    return reducerNum;
  }
  
  public JobStatus getStatus() {
    return status;
  }
  
  public void setStatus(JobStatus s) {
    this.status = s;
  }
  
  public String getPartitionerClassName() {
    return partitionerClassName;
  }

  public String getInputFormatClassName() {
    return inputFormatClassName;
  }

  public String getOutputFormatClassName() {
    return outputFormatClassName;
  }
}
