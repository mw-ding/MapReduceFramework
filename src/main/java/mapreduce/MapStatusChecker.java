package mapreduce;
import java.rmi.Remote;
import java.rmi.RemoteException;


public interface MapStatusChecker extends Remote {
  
  public enum MapStatus {
    INPROGRESS, FINISHED, FAILED
  };
  
  public MapStatus checkMapStatus(int tid) throws RemoteException;
  
}
