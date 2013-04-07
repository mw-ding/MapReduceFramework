import java.util.*;

public class TaskTrackerMeta {
	// the unique name of task tracker
	private String taskTrackerName;
	
	private TaskLauncherInterface taskLauncher;
	
	private Set<Integer> tasks;
	
	public TaskTrackerMeta(String name, TaskLauncherInterface launcher) {
		this.taskTrackerName = name;
		this.taskLauncher = launcher;
	}
	
	public String getTaskTrackerName() {
		return taskTrackerName;
	}
}
