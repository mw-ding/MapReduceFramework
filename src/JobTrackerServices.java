import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
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
		
		TaskTrackerUpdatePkg taskTracker = (TaskTrackerUpdatePkg) statusPkg;
		
		/*update the current taskTracker status*/
		Map<String, TaskTrackerMeta> allTaskTrackers = this.jobTracker.getTaskTrackers();
		String taskName = taskTracker.getTaskTrackerName();
		TaskTrackerMeta meta = allTaskTrackers.get(taskName);
		
		meta.setNumOfMapperSlots(taskTracker.getNumOfMapperSlots());
		meta.setNumOfReducerSlots(taskTracker.getNumOfReducerSlots());
		meta.setTimestamp(System.currentTimeMillis());
		
		/*update the tasks the taskTracker maintains*/
		Map<Integer, TaskMeta> allTasks = this.jobTracker.getTasks();
		
		for(TaskProgress taskProg : taskTracker.getTaskStatus()){
			TaskMeta task = allTasks.get(taskProg.getTaskID());
			task.setTaskProgress(taskProg);
			
			/*delete complete tasks*/
			Set<Integer> taskIds = meta.getTasks();
			if(taskProg.getStatus() == TaskStatus.SUCCEED){
				taskIds.remove(task.getTaskID());
			}
		}
		
	}

	@Override
	public int requestJobID() throws RemoteException {
		return this.jobTracker.requestJobId();
	}

	@Override
	public boolean submitJob(JobConf jconf) throws RemoteException {
		return true;
	}

}
