import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class JobTracker {
	
	public final static String JOBTRACKER_SERVICE_NAME = "JobTrackerService";

	// all the tasktrackers running under current system
	private Map<String, TaskTrackerMeta> tasktrackers;
	
	// all the tasks, including accomplished, executing, and uninitiated
	private Map<Integer, TaskMeta> tasks;
	
	// all the jobs, including accomplished, executing, and uninitiated
	private Map<Integer, Job> jobs;
	
	// the task queue that all tasks that have not been arranged to execute
	private Queue<TaskMeta> tasksQueue;
	
	// all the communication services that this jobtracker provides
	private JobTrackerServices services;
	
	// the scheduler, which generally decides assign which map/reduce task of
	// which job to which tasktracker.
	private TaskScheduler scheduler;
	
	private int currentMaxJobId;
	
	private int currentMaxTaskId;

	/*****************************************/
	
	public JobTracker(String rh, int rp) throws RemoteException {
		this.currentMaxJobId = 0;
		this.currentMaxTaskId = 0;
		
		this.tasktrackers = Collections.synchronizedMap(new HashMap<String, TaskTrackerMeta>());
		this.tasks = Collections.synchronizedMap(new HashMap<Integer, TaskMeta>());
		this.jobs = Collections.synchronizedMap(new HashMap<Integer, Job>());
		
		this.tasksQueue = (Queue<TaskMeta>) Collections.synchronizedCollection(new PriorityQueue<TaskMeta>(10, new Comparator<TaskMeta>() {

			@Override
			public int compare(TaskMeta o1, TaskMeta o2) {
				return o1.getTaskID() - o2.getTaskID();
			}
			
		}));
				
		this.scheduler = new DefaultTaskScheduler();
		
		this.services = new JobTrackerServices(this);
		Registry reg = LocateRegistry.getRegistry(rh, rp);
		reg.rebind(JOBTRACKER_SERVICE_NAME, this.services);
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
		return new HashMap<String, TaskTrackerMeta>(this.tasktrackers);
	}
	
	public void deleteTaskTracker(String ttname) {
		if (ttname == null) return ;
		
		if (this.tasktrackers.containsKey(ttname)) {
			this.tasktrackers.remove(ttname);
			
			// TODO : restart thoses tasks running on this tasktracker.
		}
	}
	
	public Map<Integer, TaskMeta> getTasks(){
		return this.tasks;
	}
	
	public int requestJobId() {
		int result = (++this.currentMaxJobId);
		return result;
	}
	
	public int requestTaskId() {
		int result = (++this.currentMaxTaskId);
		return result;
	}
}
