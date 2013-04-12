import java.rmi.Remote;


public interface MapStatusChecker extends Remote {
  
  public boolean isAllMapperFinished(int tid);
  
}
