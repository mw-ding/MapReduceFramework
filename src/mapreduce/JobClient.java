package mapreduce;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * This class is in charge of interact with users, like submitting jobs, kill jobs and lookup
 * status, etc.
 */
public class JobClient {

  // the remote reference to the job submitter of the job tracker;
  private JobTrackerJobSubmitter jobTrackerJobSubmitter;

  public JobClient() {
    try {
      // locate the job tracker service from the job tracker registry
      Registry register = LocateRegistry.getRegistry(Utility.getParam("JOB_TRACKER_REGISTRY_HOST"),
              Integer.parseInt(Utility.getParam("REGISTRY_PORT")));
      this.jobTrackerJobSubmitter = (JobTrackerJobSubmitter) register.lookup(Utility
              .getParam("JOB_TRACKER_SERVICE_NAME"));

      // register client service to local registry
      ClientServices cs = new ClientServices(this);
      String rHostName = Utility.getParam("CLIENT_HOST");
      Registry reg = LocateRegistry.getRegistry(rHostName,
              Integer.parseInt(Utility.getParam("REGISTRY_PORT")));
      reg.rebind(Utility.getParam("CLIENT_SERVICE_NAME"), cs);
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (NotBoundException e) {
      e.printStackTrace();
    }
  }

  /**
   * request the next available job ID for current job that is going to be submitted.
   * 
   * @return a positive integer if it is ok to submit new job a negtive integer if the system is not
   *         available
   * @throws RemoteException
   */
  private int requestJobID() {
    try {
      return this.jobTrackerJobSubmitter.requestJobID();
    } catch (RemoteException e) {
      e.printStackTrace();
      return -1;
    }
  }

  /**
   * submit a job with a configuration to job tracker
   * 
   * @param jconf
   */
  public void submitJob(JobConf jconf) {
    if (jconf == null || !jconf.isValid()) {
      System.err.println("Invalid job configuration.");
    }

    // 1. request available job id from job tracker
    int jid = this.requestJobID();
    if (jid <= 0) {
      System.err.println("The system is not available for submitting new job.");
      return;
    } else {
      jconf.setJobID(jid);
    }
    System.out.println("client get job id " + jid + " from job tracker");

    // 2. set the job id as the default job name if name has not been specified
    if (jconf.getJobName().length() == 0) {
      jconf.setJobName("Job " + Integer.toString(jid));
    }

    // 3. prepare the data and code. Not necessary for now.

    // 4. finally submit the job to the job tracker
    try {
      if (this.jobTrackerJobSubmitter.submitJob(jconf)) {
        System.out.println("Client submmited Job successfully.");
      } else {
        System.out.println("Failed to submit this job");
      }
    } catch (RemoteException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    JobClient jobClient = new JobClient();
  }
}
