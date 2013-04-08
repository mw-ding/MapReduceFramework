import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class TaskTrackerServices extends UnicastRemoteObject implements TaskLauncher, StatusUpdater{
  
  private TaskTracker taskTracker;
  
  public TaskTrackerServices(TaskTracker taskTracker) throws RemoteException{
    super();
    this.taskTracker = taskTracker;
  }

  public TaskOutput runTask(TaskInfo taskinfo) throws RemoteException{
    return null;
  }

  public void update(Object statuspck) throws RemoteException {
    
  }
}
