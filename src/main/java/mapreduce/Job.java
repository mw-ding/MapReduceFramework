package mapreduce;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Job {
  private JobConf jobConf;

  /* used to submit the job to client */
  private ClientJobSubmitter cs;

  public Job(JobConf jobConf) {
    // locate the client service from the local client registry
    this.jobConf = jobConf;
    try {
      String rHostName = Utility.getParam("CLIENT_HOST");
      Registry register = LocateRegistry.getRegistry(rHostName,
              Integer.parseInt(Utility.getParam("REGISTRY_PORT")));
      this.cs = (ClientJobSubmitter) register.lookup(Utility.getParam("CLIENT_SERVICE_NAME"));
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (NotBoundException e) {
      e.printStackTrace();
    }
  }

  /* submit job to client */
  public void run() {
    try {
      cs.submitJob(jobConf);
    } catch (RemoteException e) {
      e.printStackTrace();
    }
  }
}
