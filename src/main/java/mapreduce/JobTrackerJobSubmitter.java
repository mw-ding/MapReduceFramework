package mapreduce;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * This interface is used for JobClients to communiation with the jobtracker, including get the job
 * id first and then submit a job
 */
public interface JobTrackerJobSubmitter extends Remote {

  public int requestJobID() throws RemoteException;

  public boolean submitJob(JobConf jconf) throws RemoteException;

}
