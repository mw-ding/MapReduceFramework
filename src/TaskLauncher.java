import java.rmi.Remote;
import java.rmi.RemoteException;

public interface TaskLauncher extends Remote {
  TaskOutput runTask(TaskInfo taskinfo) throws RemoteException;
}
