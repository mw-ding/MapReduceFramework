import java.rmi.Remote;
import java.rmi.RemoteException;

public class TaskTrackerServices implements Remote, TaskLauncherInterface, StatusUpdater{

  public TaskOutput runTask(TaskInfo taskinfo) {
    return null;
  }

  public void update(Object statuspck) throws RemoteException {
    
  }
}
