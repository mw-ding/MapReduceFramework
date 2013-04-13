package mapreduce;
import java.io.File;

public class MapperTaskInfo extends TaskInfo {

  /* the input file path */
  private String inputPath;

  private long offset;

  /* the block size to be processed */
  private int blockSize;

  /* the path of the mapper or reducer */
  private String mapper;

  /* the partitioner */
  private String partitioner;

  /* the input format */
  private String inputFormat;

  /* the output path */
  private String outputPath;

  /* the number of reducer */
  private int reducerNum;

  /**
   * contructor method 
   * @param jid
   * @param taskID
   * @param inputPath
   * @param offset
   * @param blockSize
   * @param mapper
   * @param partitioner
   * @param inputFormat
   * @param jobOutputPath
   * @param reducerNum
   */
  public MapperTaskInfo(int jid, int taskID, String inputPath, long offset, int blockSize, String mapper,
          String partitioner, String inputFormat, String jobOutputPath, int reducerNum) {
    super(jid, taskID, TaskMeta.TaskType.MAPPER);
    this.inputPath = inputPath;
    this.offset = offset;
    this.blockSize = blockSize;
    this.mapper = mapper;
    this.partitioner = partitioner;
    this.inputFormat = inputFormat;
    this.reducerNum = reducerNum;

    this.outputPath = jobOutputPath + File.separator + JobTracker.TASK_MAPPER_OUTPUT_PREFIX
            + this.getTaskID();
    (new File(this.outputPath)).mkdir();
  }

  public String getInputPath() {
    return inputPath;
  }

  public long getOffset() {
    return offset;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public String getMapper() {
    return mapper;
  }

  public String getPartitioner() {
    return partitioner;
  }

  public String getInputFormat() {
    return inputFormat;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public int getReducerNum() {
    return reducerNum;
  }

}
