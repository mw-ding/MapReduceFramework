import java.util.*;

public class JobTracker {

	private Map<String, TaskTrackerMeta> tasktrackers;
	
	private Map<Integer, TaskMeta> tasks;
	
	private Map<Integer, Job> jobs;
	
	// the scheduler, which generally decides assign which map/reduce task of
	// which job to which tasktracker.
	private TaskScheduler scheduler;

	/*****************************************/
	
	public JobTracker() {
		this.tasktrackers = new HashMap<String, TaskTrackerMeta>();
				
		this.scheduler = new DefaultTaskScheduler();
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

}
