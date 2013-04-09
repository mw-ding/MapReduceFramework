import java.io.Serializable;

public class TaskInfo implements Serializable {

  private int taskID;

  private String inputPath;

  private String codePath;

  private String outputPath;

  private TaskType type;

  public TaskInfo(int taskID, String inputPath, String codePath, String outputPath, TaskType type) {
    this.taskID = taskID;
    this.inputPath = inputPath;
    this.codePath = codePath;
    this.outputPath = outputPath;
    this.type = type;
  }

  public int getTaskID() {
    return taskID;
  }

  public void setTaskID(int taskID) {
    this.taskID = taskID;
  }

  public String getInputPath() {
    return inputPath;
  }

  public void setInputPath(String inputPath) {
    this.inputPath = inputPath;
  }

  public String getCodePath() {
    return codePath;
  }

  public void setCodePath(String codePath) {
    this.codePath = codePath;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }

  public TaskType getType() {
    return type;
  }

  public void setType(TaskType type) {
    this.type = type;
  }
  
}
