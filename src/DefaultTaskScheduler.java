import java.util.*;
import java.util.Map.Entry;

public class DefaultTaskScheduler implements TaskScheduler {

  // the reference to all the tasktrakce metadata, where we could
  // find the available slots information
  private Map<String, TaskTrackerMeta> taskTrackers;

  private Queue<TaskMeta> mapTaskQueue;

  private Queue<TaskMeta> reduceTaskQueue;

  public DefaultTaskScheduler(Map<String, TaskTrackerMeta> tt, Queue<TaskMeta> mq,
          Queue<TaskMeta> rq) {
    this.taskTrackers = tt;
    this.mapTaskQueue = mq;
    this.reduceTaskQueue = rq;
  }

  @Override
  public Map<Integer, String> scheduleTask() {
    System.out.println("Scheduling tasks.");
    Map<Integer, String> result = new HashMap<Integer, String>();

    for (Entry<String, TaskTrackerMeta> entry : taskTrackers.entrySet()) {
      TaskTrackerMeta tasktracker = entry.getValue();

      synchronized (tasktracker) {
        // fill up all the mapper slots
        if (tasktracker.getNumOfMapperSlots() > 0 && !this.mapTaskQueue.isEmpty()) {
          int slotnum = tasktracker.getNumOfMapperSlots();

          for (int i = 0; i < slotnum && !this.mapTaskQueue.isEmpty(); i++) {
            TaskMeta task = this.mapTaskQueue.poll();
            result.put(task.getTaskID(), tasktracker.getTaskTrackerName());
          }
        }

        // fill up all the reducer slots
        if (tasktracker.getNumOfReducerSlots() > 0 && !this.reduceTaskQueue.isEmpty()) {
          int slotnum = tasktracker.getNumOfReducerSlots();
          
          for (int i = 0; i < slotnum && !this.reduceTaskQueue.isEmpty(); i++) {
            TaskMeta task = this.reduceTaskQueue.poll();
            result.put(task.getTaskID(), tasktracker.getTaskTrackerName());
          }
        }
      }
    }

    return result;
  }

}
