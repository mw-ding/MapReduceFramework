import java.rmi.Remote;
import java.rmi.RemoteException;


public interface MapStatusChecker extends Remote {
  
  public boolean isAllMapperFinished(int tid) throws RemoteException;
  
}
