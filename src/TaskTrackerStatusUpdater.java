import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * The status updater for the TaskTracker. Each Map/Reduce task could send its heartbeat
 * containing its status to TaskTracker, by remotely call the update() function with
 * its status wrapped in the statuspck Object.
 */
public class TaskTrackerStatusUpdater extends UnicastRemoteObject implements StatusUpdater {

	// the reference of the TaskTracker toward which this updater execute the update()
	private TaskTracker taskTracker;
	
	/*************************************/
	
	protected TaskTrackerStatusUpdater(TaskTracker tracker) throws RemoteException {
		super();
		
		this.taskTracker = tracker;
	}

	@Override
	public void update(Object statuspck) throws RemoteException {
		// TODO : do sth to the traskTracker
	}

}
