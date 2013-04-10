import java.util.*;

public class TaskTrackerMeta {
	// the time period of how long at least the slave
	// should send a heartbeat to keep it alive
	private final long ALIVE_CYCLE = 8000; // 8 seconds

	// the unique name of task tracker
	private String taskTrackerName;

	private int numOfMapperSlots;

	private int numOfReducerSlots;

	private long timestamp;

	private TaskLauncher taskLauncher;

	private Set<Integer> tasks;

	public TaskTrackerMeta(String name, TaskLauncher services) {
		this.taskTrackerName = name;
		this.taskLauncher = services;
	}
	
	public TaskLauncher getTaskLauncher() {
		return this.taskLauncher;
	}
	
	public void setTaskLauncher(TaskLauncher tl) {
		this.taskLauncher = tl;
	}

	public int getNumOfMapperSlots() {
		return numOfMapperSlots;
	}

	public void setNumOfMapperSlots(int numOfMapperSlots) {
		this.numOfMapperSlots = numOfMapperSlots;
	}

	public int getNumOfReducerSlots() {
		return numOfReducerSlots;
	}

	public void setNumOfReducerSlots(int numOfReducerSlots) {
		this.numOfReducerSlots = numOfReducerSlots;
	}

	public Set<Integer> getTasks() {
		return tasks;
	}

	public void setTasks(Set<Integer> tasks) {
		this.tasks = tasks;
	}

	public String getTaskTrackerName() {
		return taskTrackerName;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public void setTimestamp(long ctime) {
		this.timestamp = ctime;
	}
	
	public void removeTask(int id) {
		if (this.tasks.contains(id)) {
			this.tasks.remove(id);
		}
	}

	public boolean isAlive() {
		return (System.currentTimeMillis() - this.timestamp <= ALIVE_CYCLE);
	}
}
