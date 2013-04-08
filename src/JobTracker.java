import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class JobTracker {

	private Map<String, TaskTrackerMeta> tasktrackers;
	
	private Map<Integer, TaskMeta> tasks;
	
	private Map<Integer, Job> jobs;
	
	// all the communication services that this jobtracker provides
	private JobTrackerServices services;
	
	// the scheduler, which generally decides assign which map/reduce task of
	// which job to which tasktracker.
	private TaskScheduler scheduler;

	/*****************************************/
	
	public JobTracker() throws RemoteException {
		this.tasktrackers = new HashMap<String, TaskTrackerMeta>();
				
		this.scheduler = new DefaultTaskScheduler();
		
		// TODO : register this object in the RMI registry
		this.services = new JobTrackerServices(this);
	}
	
	/**
	 * Change the task scheduler at runtime
	 * 
	 * @param s
	 * 		a new scheduler
	 */
	public void setScheduler(TaskScheduler s) {
		this.scheduler = s;
	}
	
	/**
	 * Register a new tasktracker in this jobtracker
	 * @param tt
	 * 		the metadata of this trasktracker
	 */
	public void registerTaskTracker(TaskTrackerMeta tt) {
		if (tt == null) return ;
		
		if (this.tasktrackers.containsKey(tt.getTaskTrackerName())) {
			throw new RuntimeException("The TaskTracker \"" + tt.getTaskTrackerName() + "\" already exist.");
		} else {
			this.tasktrackers.put(tt.getTaskTrackerName(), tt);
		}
	}
	
	
	public Map<String, TaskTrackerMeta> getTaskTrackers() {
		return null;
	}
	
	public void deleteTaskTracker(String ttname) {
		
	}
}
