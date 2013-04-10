import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Job {
  private JobConf jobConf;

  private ClientJobSubmitter cs;

  public Job(JobConf jobConf) {
    this.jobConf = jobConf;
    try {
      // locate the remote reference from the registry
      Registry register = LocateRegistry.getRegistry(Utility.getParam("REGISTRY_HOST"),
              Integer.parseInt(Utility.getParam("REGISTRY_PORT")));
      this.cs = (ClientJobSubmitter) register.lookup(Utility.getParam("CLIENT_SERVICE_NAME"));
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (NotBoundException e) {
      e.printStackTrace();
    }
  }

  public void run() {
    try {
      cs.submitJob(jobConf);
    } catch (RemoteException e) {
      e.printStackTrace();
    }
  }
}
