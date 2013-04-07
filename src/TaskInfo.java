import java.io.Serializable;

public class TaskInfo implements Serializable {
  String inputPath;

  String codePath;

  String outputPath;

  TaskType type;

  public TaskInfo(String inputPath, String codePath, String outputPath, TaskType type) {
    this.inputPath = inputPath;
    this.codePath = codePath;
    this.outputPath = outputPath;
    this.type = type;
  }
}
