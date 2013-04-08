import java.util.Map;

public class TaskStatusChecker implements Runnable {

  private final long ALIVE_CYCLE = 8000; // 8 seconds

  private TaskTracker taskTracker;

  public TaskStatusChecker(TaskTracker taskTracker) {
    this.taskTracker = taskTracker;
  }

  public void run() {
    checkStatus();
  }

  private void checkStatus() {
    Map<String, TaskProgress> taskStatus = this.taskTracker.getTaskStatus();
    synchronized (taskStatus) {
      for (TaskProgress taskProgress : taskStatus.values()) {
        long curTime = System.currentTimeMillis();
        if ((curTime - taskProgress.getTimestamp() > this.ALIVE_CYCLE)
                && taskProgress.status != TaskStatus.SUCCEED) {
          taskProgress.status = TaskStatus.FAILED;
        }
      }
    }
  }

}
