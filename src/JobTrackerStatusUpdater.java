import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * The status updater for the JobTracker. Each TaskTracker could send its heartbeat
 * containing its status to JobTracker, by remotely call the update() function with
 * its status wrapped in the statuspck Object.
 */
public class JobTrackerStatusUpdater extends UnicastRemoteObject implements StatusUpdater {

	// the reference of the JobTracker toward which this updater execute the update()
	private JobTracker jobTracker;
	
	
	
	/*************************************/
	
	protected JobTrackerStatusUpdater(JobTracker tracker) throws RemoteException {
		super();
		
		this.jobTracker = tracker;
	}

	@Override
	public void update(Object statuspck) throws RemoteException {
		// TODO : do sth to the jobTracker
	}

}
