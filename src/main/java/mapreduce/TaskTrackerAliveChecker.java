package mapreduce;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

public class TaskTrackerAliveChecker implements Runnable {

  // the reference of the job tracker
  private JobTracker jTracker;

  public TaskTrackerAliveChecker(JobTracker j) {
    this.jTracker = j;
  }

  @Override
  public void run() {
    Map<String, TaskTrackerMeta> taskTrackers = this.jTracker.getTaskTrackers();

    for (Map.Entry<String, TaskTrackerMeta> entry : taskTrackers.entrySet()) {
      TaskTrackerMeta meta = entry.getValue();

      // if this task tracker is now not alive anymore, remove it
      if (!meta.isAlive()) {
        this.jTracker.deleteTaskTracker(meta.getTaskTrackerName());
        
        // reassign the tasks running on this task tracker
        Set<Integer> currentTasks = meta.getTasks();
        for (Integer tid : currentTasks) {
          this.jTracker.submitExistingTask(tid);
        }
      }
    }

  }

}
