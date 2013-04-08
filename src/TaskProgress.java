import java.io.Serializable;

public class TaskProgress implements Serializable {
  String taskID;

  float percentage;

  TaskStatus status;
  
  long timestamp;
  
  public TaskProgress(String taskID) {
    this.taskID = taskID;
  }

  public float getPercentage() {
    return percentage;
  }

  public void setPercentage(float percentage) {
    this.percentage = percentage;
  }
  

  public TaskStatus getStatus() {
    return status;
  }

  public void setStatus(TaskStatus status) {
    this.status = status;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

}
