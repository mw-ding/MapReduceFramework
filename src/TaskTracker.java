import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import test.HelloInterface;

public class TaskTracker implements TaskLauncherInterface{
  private String registryHostName;

  private int registryPort;

  private String taskTrackerName;

  private int numOfMapperSlots;

  private int numOfReducerSlots;

  private StatusUpdater jobTrackerStatusUpdater;

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
      jobTrackerStatusUpdater = (StatusUpdater) reg.lookup("jobTrackerStatusUpdaterName");
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (NotBoundException e) {
      e.printStackTrace();
    }
    
    /* initiate slots status */
    
  }

  /**
   * register it to rmi registry
   */
  public void registerLuancher() {
    try {
      Registry reg = LocateRegistry.getRegistry(this.registryHostName, this.registryPort);
      reg.bind(this.taskTrackerName, this);
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
