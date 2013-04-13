package mapreduce;
import java.rmi.Remote;
import java.rmi.RemoteException;
/**
 * This interface is used to submit task to task tracker 
 *
 */
public interface TaskLauncher extends Remote {
  boolean runTask(TaskInfo taskinfo) throws RemoteException;
}
