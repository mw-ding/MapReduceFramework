import java.io.Serializable;

public abstract class TaskInfo implements Serializable {

  /* the id of the task */
  private int taskID;

  /* worker type, mapper or reducer */
  private TaskType type;

  public TaskInfo(int taskID, TaskType type) {
    this.taskID = taskID;
    this.type = type;
  }

  public int getTaskID() {
    return taskID;
  }

  public TaskType getType() {
    return type;
  }

}
