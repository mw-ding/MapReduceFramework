import java.io.Serializable;

public class TaskInfo implements Serializable {

  private int taskID;

  private String inputPath;

  private long offset;

  private int blockSize;

  private String codePath;

  private String outputPath;

  private TaskType type;

  public TaskInfo(int taskID, String inputPath, long offset, int blockSize, String codePath,
          String outputPath, TaskType type) {
    this.taskID = taskID;
    this.inputPath = inputPath;
    this.offset = offset;
    this.blockSize = blockSize;
    this.codePath = codePath;
    this.outputPath = outputPath;
    this.type = type;
  }

  public int getTaskID() {
    return taskID;
  }

  public String getInputPath() {
    return inputPath;
  }

  public String getCodePath() {
    return codePath;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public TaskType getType() {
    return type;
  }

  public long getOffset() {
    return offset;
  }

  public int getBlockSize() {
    return blockSize;
  }

}
