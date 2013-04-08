import java.util.Map;
import java.util.Set;
import java.util.Iterator;

public class TaskTrackerAliveChecker implements Runnable {
	
	private JobTracker jTracker;
	
	public TaskTrackerAliveChecker(JobTracker j){
		
		this.jTracker = j;
		
		Thread taskTrackerChecker = new Thread();
		taskTrackerChecker.start();
		
	}
	
	@Override
	public void run() {
		
		Map<String, TaskTrackerMeta> taskTrackers = this.jTracker.getTaskTrackers();
		
		for (Map.Entry<String, TaskTrackerMeta> entry : taskTrackers.entrySet()) {
			
			TaskTrackerMeta meta = entry.getValue();
			
			if(!meta.isAlive(meta.getTimestamp()))
				this.jTracker.deleteTaskTracker(meta.getTaskTrackerName());
        }
		
	}

}
