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

  public MapperTaskInfo(int taskID, String inputPath, long offset, int blockSize, String mapper,
          String partitioner, String inputFormat, String outputPath, int reducerNum) {
    super(taskID, TaskType.MAPPER);
    this.inputPath = inputPath;
    this.offset = offset;
    this.blockSize = blockSize;
    this.mapper = mapper;
    this.partitioner = partitioner;
    this.inputFormat = inputFormat;
    this.reducerNum = reducerNum;
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
