import java.util.*;


public class Job {
	// the unique name for each map/reduce job
	private String jobName;
	
	private Set<Integer> tasks;
	
	public Job() {
		tasks = new HashSet<Integer>();
	}
	
	public String getJobName() {
		return this.jobName;
	}
}
