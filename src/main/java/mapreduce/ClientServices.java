package mapreduce;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 *  this service is used to submit job to client, then client submit this job to job tracker
 *  */
public class ClientServices extends UnicastRemoteObject implements ClientJobSubmitter {

  private JobClient jobClient;

  protected ClientServices(JobClient jobClient) throws RemoteException {
    super();
    this.jobClient = jobClient;
  }

  public void submitJob(JobConf jconf) throws RemoteException {
    jobClient.submitJob(jconf);
  }

}
