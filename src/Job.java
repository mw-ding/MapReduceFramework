import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Job {
  private JobConf jobConf;
  private ClientServices cs;

  public Job(JobConf jobConf, String rh, int rp) {
    this.jobConf = jobConf;
    try {
      // locate the remote reference from the registry
      Registry register = LocateRegistry.getRegistry(rh, rp);
      this.cs = (ClientServices) register.lookup(JobClient.CLIENT_SERVICE_NAME);
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
