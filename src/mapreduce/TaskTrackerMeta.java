package mapreduce;

import java.util.*;

public class TaskTrackerMeta {
  // the time period of how long at least the slave
  // should send a heartbeat to keep it alive
  private final int ALIVE_CYCLE; // 8 seconds

  // the unique name of task tracker
  private String taskTrackerName;

  private int numOfMapperSlots;

  private int numOfReducerSlots;

  private long timestamp;

  private TaskLauncher taskLauncher;

  private Set<Integer> tasks;

  public TaskTrackerMeta(String name, TaskLauncher services) {
    this.taskTrackerName = name;
    this.taskLauncher = services;
    this.tasks = new HashSet<Integer>();
    this.numOfMapperSlots = 0;
    this.numOfReducerSlots = 0;
    this.ALIVE_CYCLE = Integer.parseInt(Utility.getParam("ALIVE_CYCLE"));
  }

  public TaskLauncher getTaskLauncher() {
    return this.taskLauncher;
  }

  public int getNumOfMapperSlots() {
    return numOfMapperSlots;
  }

  public void setNumOfMapperSlots(int numOfMapperSlots) {
    this.numOfMapperSlots = numOfMapperSlots;
  }

  public int getNumOfReducerSlots() {
    return numOfReducerSlots;
  }

  public void setNumOfReducerSlots(int numOfReducerSlots) {
    this.numOfReducerSlots = numOfReducerSlots;
  }

  public Set<Integer> getTasks() {
    return tasks;
  }

  public String getTaskTrackerName() {
    return taskTrackerName;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public void setTimestamp(long ctime) {
    this.timestamp = ctime;
  }

  public void removeTask(int id) {
    if (this.tasks.contains(id)) {
      this.tasks.remove(id);
    }
  }

  public boolean isAlive() {
    return (System.currentTimeMillis() - this.timestamp <= ALIVE_CYCLE);
  }
}
