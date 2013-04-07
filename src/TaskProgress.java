import java.io.Serializable;

public class TaskProgress implements Serializable {
  String taskID;

  float percentage;

  boolean failed;

  public TaskProgress(String taskID, float percentage, boolean failed) {
    this.taskID = taskID;
    this.percentage = percentage;
    this.failed = failed;
  }

}
