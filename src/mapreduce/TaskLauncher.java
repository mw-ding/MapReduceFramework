package mapreduce;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface TaskLauncher extends Remote {
  boolean runTask(TaskInfo taskinfo) throws RemoteException;
}
