import java.io.Serializable;

public class TaskInfo implements Serializable {

  String taskID;

  String inputPath;

  String codePath;

  String outputPath;

  TaskType type;

  public TaskInfo(String taskID, String inputPath, String codePath, String outputPath, TaskType type) {
    this.taskID = taskID;
    this.inputPath = inputPath;
    this.codePath = codePath;
    this.outputPath = outputPath;
    this.type = type;
  }
}
