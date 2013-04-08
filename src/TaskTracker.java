import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;

import test.HelloInterface;

public class TaskTracker {
  private String registryHostName;

  private int registryPort;

  private String taskTrackerName;

  private int numOfMapperSlots;

  private int numOfReducerSlots;

  private StatusUpdater jobTrackerStatusUpdater;
  
  private HashMap<String, TaskProgress> taskStatus;

  /**
   * constructor of task tracker
   * 
   * @param rHostName
   * @param rPort
   * @param taskTrackerName
   * @param jobTrackerStatusUpdaterName
   */
  public void TaskTracker(String rHostName, int rPort, String taskTrackerName,
          int numOfMapperSlots, int numOfReducerSlots, String jobTrackerStatusUpdaterName) {
    this.registryHostName = rHostName;
    this.registryPort = rPort;
    this.taskTrackerName = taskTrackerName;
    this.numOfMapperSlots = numOfMapperSlots;
    this.numOfReducerSlots = numOfReducerSlots;

    /* get the job tracker status updater */
    try {
      Registry reg = LocateRegistry.getRegistry(this.registryHostName, this.registryPort);
      jobTrackerStatusUpdater = (StatusUpdater) reg.lookup(jobTrackerStatusUpdaterName);
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (NotBoundException e) {
      e.printStackTrace();
    }

    /* initiate task status */
    this.taskStatus = new HashMap<String, TaskProgress>();
    
  }

  /**
   * register services to rmi registry
   */
  private void registerServices() {
    try {
      TaskTrackerServices tts = new TaskTrackerServices(this);
      Registry reg = LocateRegistry.getRegistry(this.registryHostName, this.registryPort);
      reg.bind(this.taskTrackerName, tts);
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (AlreadyBoundException e) {
      e.printStackTrace();
    }
  }

  public TaskOutput runTask(TaskInfo taskinfo) {
    return null;
  }

}
