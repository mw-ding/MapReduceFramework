import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;

public class JobTrackerServices extends UnicastRemoteObject implements StatusUpdater, JobSubmitter {

	private JobTracker jobTracker;
	
	protected JobTrackerServices(JobTracker jt) throws RemoteException {
		super();
		this.jobTracker = jt;
	}

	@Override
	public void update(Object statusPkg) throws RemoteException {
		
		/*check update package class*/
		if(statusPkg.getClass().getName().compareTo(TaskTrackerUpdatePkg.class.getName())!=0)
			return;
		
		TaskTrackerUpdatePkg taskTrackerPkg = (TaskTrackerUpdatePkg) statusPkg;
		
		/*update the current taskTracker status*/
		String taskName = taskTrackerPkg.getTaskTrackerName();
		TaskTrackerMeta ttmeta = this.jobTracker.getTaskTracker(taskName);
		if (ttmeta == null) {
			System.err.println("TaskTracker " + taskName + " does not exist.");
			return ;
		}
		
		ttmeta.setNumOfMapperSlots(taskTrackerPkg.getNumOfMapperSlots());
		ttmeta.setNumOfReducerSlots(taskTrackerPkg.getNumOfReducerSlots());
		ttmeta.setTimestamp(System.currentTimeMillis());
		
		/*update the tasks the taskTracker maintains*/
		List<TaskProgress> taskStatus = taskTrackerPkg.getTaskStatus();
		Map<Integer, TaskMeta> allMapTasks = this.jobTracker.getMapTasks();
		Map<Integer, TaskMeta> allReduceTasks = this.jobTracker.getReduceTasks();
		
		for(TaskProgress taskProg : taskStatus){
			int taskid = taskProg.getTaskID();
			TaskMeta task = null;
			
			/* check whether task exist */
			if (allMapTasks.containsKey(taskid)) {
				task = allMapTasks.get(taskid);
			} else if (allReduceTasks.containsKey(taskid)) {
				task = allReduceTasks.get(taskid);
			}
			
			if (task == null) {
				System.err.println("Task " + taskid + " does not exist.");
				continue;
			}
			
			task.setTaskProgress(taskProg);
			/*delete this task from the task tracker if it completes*/
			if(taskProg.getStatus() == TaskStatus.SUCCEED){
				ttmeta.removeTask(taskProg.getTaskID());
			}
		}
		
		// TODO : check whether a job finishes
	}

	@Override
	public int requestJobID() throws RemoteException {
		return this.jobTracker.requestJobId();
	}

	@Override
	public boolean submitJob(JobConf jconf) throws RemoteException {
		// TODO : 1. split the input
		// TODO : 2. trigger the task scheduler
		return true;
	}

}
