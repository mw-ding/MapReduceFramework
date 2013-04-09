import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;


/**
 * This class is in charge of interact with users, like submitting jobs, kill
 * jobs and lookup status, etc.
 */
public class JobClient {
	
	// the remote reference to the job submitter of the job tracker;
	private JobSubmitter jobsubmitter;
	
	public JobClient(String rh, int rp) {
		try {
			// locate the remote reference from the registry
			Registry register = LocateRegistry.getRegistry(rh, rp);
			this.jobsubmitter = (JobSubmitter) register.lookup(JobTracker.JOBTRACKER_SERVICE_NAME);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * request the next available job ID for current job that is going to be
	 * submitted.
	 * 
	 * @return 
	 * 		a positive integer if it is ok to submit new job
	 * 		a negtive integer if the system is not available
	 * @throws RemoteException
	 */
	private int requestJobID() {
		try {
			return this.jobsubmitter.requestJobID();
		} catch (RemoteException e) {
			e.printStackTrace();
			return -1;
		}
	}

	/**
	 * submit a job with a configuration
	 * @param jconf
	 */
	private void submitJob(JobConf jconf) {
		if (jconf == null || !jconf.isValid()) {
			System.err.println("Invalid job configuration.");
		}
		
		// 1. request available job id from job tracker
		int jid = this.requestJobID();
		if (jid <= 0) {
			System.err.println("The system is not available for submitting new job.");
		}
		
		// 2. set the job id as the default job name if name has not been specified
		if (jconf.getJobName().length() == 0) {
			jconf.setJobName("Job " + Integer.toString(jid));
		}
		
		// 3. prepare the data and code. Not necessary for now.
		
		// 4. finally submit the job to the job tracker
		try {
			if (this.jobsubmitter.submitJob(jconf)) {
				System.out.println("Job submitted.");
			} else {
				System.out.println("Failed to submit this job");
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		while(true) {
			// TODO : 
		}
	}
}
