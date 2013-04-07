public class JobTracker {

	// the scheduler, which generally decides assign which map/reduce task of
	// which job to which tasktracker.
	private TaskScheduler scheduler;

	/*****************************************/
	
	public JobTracker() {
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

}
