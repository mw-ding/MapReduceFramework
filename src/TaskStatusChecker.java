 import java.util.Map;

public class TaskStatusChecker implements Runnable {
  
  private final long ALIVE_CYCLE = 8000; // 8 seconds

  private TaskTracker taskTracker;

  public TaskStatusChecker(TaskTracker taskTracker) {
    this.taskTracker = taskTracker;
  }

  public void run() {

  }

  private void checkStatus() {
    Map<String, TaskProgress> taskStatus = this.taskTracker.getTaskStatus();
    synchronized(taskStatus){
      for(TaskProgress taskProgress : taskStatus.values()){
        if(System.currentTimeMillis() - taskProgress.getTimestamp() <= this.ALIVE_CYCLE){
          taskProgress.status = TaskStatus.INPROGRESS;
        }
      }
    }
  }

}
