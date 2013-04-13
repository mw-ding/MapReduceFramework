import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class TaskStatusChecker implements Runnable {

  private final long ALIVE_CYCLE = 8000; // 8 seconds
  
  private final int HEART_BEAT_PERIOD = 2000;

  private TaskTracker taskTracker;

  public TaskStatusChecker(TaskTracker taskTracker) {
    this.taskTracker = taskTracker;
  }

  public void run() {
    checkStatus();
  }

  private void checkStatus() {
    ScheduledExecutorService schExec = Executors.newScheduledThreadPool(8);
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        Map<Integer, TaskProgress> taskStatus = taskTracker.getTaskStatus();
        synchronized (taskStatus) {
          for (TaskProgress taskProgress : taskStatus.values()) {
            long curTime = System.currentTimeMillis();
            if ((curTime - taskProgress.getTimestamp() > ALIVE_CYCLE)
                    && taskProgress.getStatus() != TaskStatus.SUCCEED) {
              System.out.println("#### set task " + taskProgress.getTaskID() + " failed.");
            }
              taskProgress.setStatus(TaskStatus.FAILED);
            }
          }
        }
    });
    thread.setDaemon(true);
    ScheduledFuture<?> schFuture = schExec.scheduleAtFixedRate(thread, 0, HEART_BEAT_PERIOD,
            TimeUnit.SECONDS);

  }

}
