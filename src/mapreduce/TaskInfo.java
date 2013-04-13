package mapreduce;
import java.io.Serializable;

public abstract class TaskInfo implements Serializable {

  private int jobID;
  
  /* the id of the task */
  private int taskID;

  /* worker type, mapper or reducer */
  private TaskMeta.TaskType type;

  public TaskInfo(int jid, int tid, TaskMeta.TaskType type) {
    this.jobID = jid;
    this.taskID = tid;
    this.type = type;
  }
  
  public int getJobID() {
    return jobID;
  }

  public int getTaskID() {
    return taskID;
  }

  public TaskMeta.TaskType getType() {
    return type;
  }

}
