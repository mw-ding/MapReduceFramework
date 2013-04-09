import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class JobTrackerServices extends UnicastRemoteObject implements StatusUpdater, JobSubmitter {

	private JobTracker jobTracker;
	
	protected JobTrackerServices(JobTracker jt) throws RemoteException {
		super();
		this.jobTracker = jt;
	}

	@Override
	public void update(Object statuspck) throws RemoteException {
	}

	@Override
	public int requestJobID() throws RemoteException {
		return 0;
	}

	@Override
	public boolean submitJob(JobConf jconf) throws RemoteException {
		return true;
	}

}
