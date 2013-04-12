import java.io.Serializable;

public class TaskProgress implements Serializable {
  private int taskID;

  private float percentage;

  private TaskStatus status;

  private TaskType type;

  private long timestamp;

  public TaskProgress(int taskID, TaskType type) {
    this.taskID = taskID;
    this.status = TaskStatus.INIT;
    this.type = type;
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

  public int getTaskID() {
    return taskID;
  }

  public void setTaskID(int taskID) {
    this.taskID = taskID;
  }

  public TaskType getType() {
    return this.type;
  }
}
