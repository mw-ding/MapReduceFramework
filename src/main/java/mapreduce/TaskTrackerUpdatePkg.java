package mapreduce;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.List;

/**
 * This class is used to update task tracker status to job tracker.
 * 
 */
public class TaskTrackerUpdatePkg implements Serializable {

  /* the name of the task tracker */
  private String taskTrackerName;

  /* the number of maper slots still available */
  private int numOfMapperSlots;

  /* the number of reducer slots still available */
  private int numOfReducerSlots;

  /* the status of tasks running on task tracker */
  private List<TaskProgress> taskStatus;

  /* the service name of task tracker: where to contact of necessary */
  private String serviceName;

  /* the task tracker registry host name */
  private String rmiHostName;

  /* the task tracker registry port */
  private int rmiPort;

  /**
   * constructor method
   * 
   * @param taskTrackerName
   * @param numOfMapperSlots
   * @param numOfReducerSlots
   * @param sn
   * @param taskStatus
   */
  public TaskTrackerUpdatePkg(String taskTrackerName, int numOfMapperSlots, int numOfReducerSlots,
          String sn, List<TaskProgress> taskStatus, int rmiport) {
    this.taskTrackerName = taskTrackerName;
    this.numOfMapperSlots = numOfMapperSlots;
    this.numOfReducerSlots = numOfReducerSlots;
    this.taskStatus = taskStatus;
    this.serviceName = sn;

    try {
      this.rmiHostName = java.net.InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    this.rmiPort = rmiport;
  }

  public int getNumOfMapperSlots() {
    return numOfMapperSlots;
  }

  public int getNumOfReducerSlots() {
    return numOfReducerSlots;
  }

  public List<TaskProgress> getTaskStatus() {
    return taskStatus;
  }

  public String getTaskTrackerName() {
    return taskTrackerName;
  }

  public String getServiceName() {
    return serviceName;
  }

  public String getRmiHostName() {
    return rmiHostName;
  }

  public int getRmiPort() {
    return rmiPort;
  }
}
