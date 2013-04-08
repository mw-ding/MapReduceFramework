import java.util.*;

public class TaskTrackerMeta {
	// the unique name of task tracker
	private String taskTrackerName;
	
	private long timestamp;
	
	private TaskTrackerServices taskTrackServices;
	
	private Set<Integer> tasks;
	
	public TaskTrackerMeta(String name, TaskTrackerServices services) {
		this.taskTrackerName = name;
		this.taskTrackServices = services;
	}
	
	public String getTaskTrackerName() {
		return taskTrackerName;
	}
	
	public long getTimestamp() {
		return this.timestamp;
	}
	
	public boolean isAlive(long ctime) {
		return true;
	}
}
