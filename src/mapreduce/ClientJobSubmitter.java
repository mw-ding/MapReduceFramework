package mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * This interface is used to submit job to client with rmi
 */
public interface ClientJobSubmitter extends Remote {
  public void submitJob(JobConf jconf) throws RemoteException;
}
