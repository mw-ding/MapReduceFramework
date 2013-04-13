package mapreduce;
import java.io.Serializable;

public class JobConf implements Serializable {
  /* TODO: make set job id invisible to user */

  // the id for current job
  private int jobID;

  // the name for current job
  private String jobName;

  // the input path
  private String inputPath;

  // the output path
  private String outputPath;
  
  // the path of the jar file which contains all the codes of a job
  private String jarFilePath;

  // the block size
  private int blockSize;

  // the number of reducers
  private int reducerNum;

  // the mapper class name
  private String mapperClassName;

  // the reducer class name
  private String reducerClassName;

  // the partitioner class name
  private String partitionerClassName;
  
  // the inputformat class name
  private String inputFormatClassName;
  
  // the outputformat class name
  private String outputFormatClassName;

  public JobConf() {
    this.jobName = "";
    this.inputPath = null;
    this.outputPath = null;
    this.mapperClassName = null;
    this.reducerClassName = null;
    this.partitionerClassName = null;
    this.partitionerClassName = null;
    this.inputFormatClassName = null;
    this.outputFormatClassName = null;
  }

  /**
   * whether current jobs configuration is valid for a job
   * 
   * @return
   */
  public boolean isValid() {
    // if any of the following field is null, then invalid
    if (this.inputPath == null)
      return false;

    if (this.outputPath == null)
      return false;
    
    if (this.jarFilePath == null)
      return false;

    if (this.mapperClassName == null)
      return false;

    if (this.reducerClassName == null)
      return false;

    if (this.partitionerClassName == null)
      return false;
    
    if (this.inputFormatClassName == null)
      return false;
    
    if (this.outputFormatClassName == null)
      return false;

    if (this.blockSize == 0)
      return false;

    if (this.reducerNum == 0)
      return false;

    return true;
  }

  // the getters and setters for all members
  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getInputPath() {
    return inputPath;
  }

  public void setInputPath(String inputPath) {
    this.inputPath = inputPath;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }

  public int getJobID() {
    return jobID;
  }

  public void setJobID(int jobID) {
    this.jobID = jobID;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public void setBlockSize(int blockSize) {
    this.blockSize = blockSize;
  }

  public String getMapperClassName() {
    return mapperClassName;
  }

  public void setMapperClassName(String mapperClassName) {
    this.mapperClassName = mapperClassName;
  }

  public String getReducerClassName() {
    return reducerClassName;
  }

  public void setReducerClassName(String reducerClassName) {
    this.reducerClassName = reducerClassName;
  }

  public int getReducerNum() {
    return reducerNum;
  }

  public void setReducerNum(int reducerNum) {
    this.reducerNum = reducerNum;
  }

  public String getPartitionerClassName() {
    return partitionerClassName;
  }

  public void setPartitionerClassName(String partitionerClassName) {
    this.partitionerClassName = partitionerClassName;
  }
  
  public String getInputFormatClassName() {
    return inputFormatClassName;
  }

  public void setInputFormatClassName(String inputFormatClassName) {
    this.inputFormatClassName = inputFormatClassName;
  }
  
  public String getOutputFormatClassName() {
    return outputFormatClassName;
  }

  public void setOutputFormatClassName(String outputFormatClassName) {
    this.outputFormatClassName = outputFormatClassName;
  }
  
  public String getJarFilePath() {
    return this.jarFilePath;
  }
  
  public void setJarFilePath(String path) {
    this.jarFilePath = path;
  }
}
