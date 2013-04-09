import java.io.Serializable;

public class TaskProgress implements Serializable {
  private int taskID;

  private float percentage;

  private TaskStatus status;
  
  private long timestamp;
  
  public TaskProgress(int taskID) {
    this.taskID = taskID;
    this.status = TaskStatus.INPROGRESS;
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
  

}
