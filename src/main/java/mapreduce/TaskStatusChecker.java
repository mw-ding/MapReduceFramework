package mapreduce;

import java.util.Map;

/**
 * this class is used to periodically check the time stamp of tasks
 * set tasks status as Failed, if no heart beat is received during ALIVE_CYCLE
 */
public class TaskStatusChecker implements Runnable {

  /* to decide if a status is alive or not: if more than ALIVE_CYCLE time
   * has passed since the last heart beat, the task is set to Failed
   * */
  private final long ALIVE_CYCLE;

  /* reference to task tracker */
  private TaskTracker taskTracker;

  /**
   * constructor method
   * @param taskTracker
   */
  public TaskStatusChecker(TaskTracker taskTracker) {
    this.taskTracker = taskTracker;
    ALIVE_CYCLE = Long.parseLong(Utility.getParam("ALIVE_CYCLE"));
  }
  
  /**
   * periodically check time stamp and set status
   */
  public void run() {
    Map<Integer, TaskProgress> taskStatus = taskTracker.getTaskStatus();
    synchronized (taskStatus) {
      for (TaskProgress taskProgress : taskStatus.values()) {
        long curTime = System.currentTimeMillis();
        if ((curTime - taskProgress.getTimestamp() > ALIVE_CYCLE)
                && taskProgress.getStatus() != TaskMeta.TaskStatus.SUCCEED) {
          taskProgress.setStatus(TaskMeta.TaskStatus.FAILED);
        }
      }
    }
  }

}
