import java.util.*;


public class JobMeta {
	// the id for the job
	private int jobId;
	
	// the name for each map/reduce job
	private String jobName;
	
	private String mapperClassFile;
	
	private String reducerClassFile;
	
	private String inputFile;
	
	private String outputFile;
	
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
