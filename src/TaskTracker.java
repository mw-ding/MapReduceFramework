import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class TaskTracker {
  private String registryHostName;

  private int registryPort;

  private TaskLauncher taskLauncher;

  private String taskTrackerName;

  /**
   * constructor of task tracker
   * 
   * @param rHostName
   * @param rPort
   * @param taskTrackerName
   */
  public void TaskTracker(String rHostName, int rPort, String taskTrackerName) {
    this.registryHostName = rHostName;
    this.registryPort = rPort;
    this.taskTrackerName = taskTrackerName;
  }

  /**
   * new a task launcher and register it to rmi registry
   */
  public void registerLuancher() {
    TaskLauncherInterface taskLauncher = new TaskLauncher();
    Registry reg;
    try {
      reg = LocateRegistry.getRegistry(this.registryHostName, this.registryPort);
      reg.bind(this.taskTrackerName, taskLauncher);
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (AlreadyBoundException e) {
      e.printStackTrace();
    }
  }
  
}
