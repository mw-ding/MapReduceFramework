package mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ClientJobSubmitter extends Remote {
  public void submitJob(JobConf jconf) throws RemoteException;
}
