package mapreduce;
import java.io.Serializable;
/**
 * The task info submitted by job tracker to task tracker
 *
 */
public abstract class TaskInfo implements Serializable {

  /* the job id */
  private int jobID;
  
  /* the id of the task */
  private int taskID;

  /* worker type, mapper or reducer */
  private TaskMeta.TaskType type;

  /**
   * constructor method 
   * @param jid
   * @param tid
   * @param type
   */
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
