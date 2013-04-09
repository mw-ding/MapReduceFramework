import java.util.*;


public class JobMeta {
	private int jobId;
	
	// the name for each map/reduce job
	private String jobName;
	
	private Set<Integer> mapTasks;
	
	private Set<Integer> reduceTasks;
	
	public JobMeta() {
		this.mapTasks = new HashSet<Integer>();
		this.reduceTasks = new HashSet<Integer>();
	}
	
	public String getJobName() {
		return this.jobName;
	}
}
