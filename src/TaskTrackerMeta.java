import java.util.*;

public class TaskTrackerMeta {
	// the time period of how long at least the slave
	// should send a heartbeat to keep it alive
	private final long ALIVE_CYCLE = 8000; // 8 seconds
	
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
	
	public boolean isAlive() {
		return (System.currentTimeMillis() - this.timestamp <= ALIVE_CYCLE);
	}
}
