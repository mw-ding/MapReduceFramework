package mapreduce;
import java.util.*;
import java.util.Map.Entry;

public class DefaultTaskScheduler implements TaskScheduler {

  // the reference to all the tasktrakce metadata, where we could
  // find the available slots information
  private Map<String, TaskTrackerMeta> taskTrackers;

  private JobTracker jobTracker;

  public DefaultTaskScheduler(JobTracker tracker) {
    this.jobTracker = tracker;
    this.taskTrackers = this.jobTracker.getTaskTrackers();
  }

  @Override
  public Map<Integer, String> scheduleTask() {
    Map<Integer, String> result = new HashMap<Integer, String>();

    for (Entry<String, TaskTrackerMeta> entry : taskTrackers.entrySet()) {
      TaskTrackerMeta tasktracker = entry.getValue();

      synchronized (tasktracker) {
        // fill up all the mapper slots
        if (tasktracker.getNumOfMapperSlots() > 0) {
          int slotnum = tasktracker.getNumOfMapperSlots();

          TaskMeta task = null;
          for (int i = 0; i < slotnum && (task = this.jobTracker.getNextMapperTask()) != null; i++) {
            result.put(task.getTaskID(), tasktracker.getTaskTrackerName());
          }
        }

        // fill up all the reducer slots
        if (tasktracker.getNumOfReducerSlots() > 0) {
          int slotnum = tasktracker.getNumOfReducerSlots();

          TaskMeta task = null;
          for (int i = 0; i < slotnum && (task = this.jobTracker.getNextReducerTask()) != null; i++) {
            result.put(task.getTaskID(), tasktracker.getTaskTrackerName());
          }
        }
      }
    }

    return result;
  }

}
