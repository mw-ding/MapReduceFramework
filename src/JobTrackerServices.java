import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class JobTrackerServices extends UnicastRemoteObject implements StatusUpdater{

	private JobTracker jobTracker;
	
	protected JobTrackerServices(JobTracker jt) throws RemoteException {
		super();
		this.jobTracker = jt;
	}

	@Override
	public void update(Object statuspck) throws RemoteException {
		
	}

}
