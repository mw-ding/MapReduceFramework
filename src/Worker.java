import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public abstract class Worker {

	private String taskId;
	private TaskProgress progress;
	private StatusUpdater taskStatusUpdater;
	
	
	public Worker(TaskInfo t, String regHostName, int regPort, String taskStatusUpdaterName){
		
		this.taskId = t.taskID;
		this.progress = new TaskProgress(this.taskId);
		
		/* get the task-tracker status updater */
	    try {
	      Registry reg = LocateRegistry.getRegistry(regHostName, regPort);
	      taskStatusUpdater = (StatusUpdater) reg.lookup(taskStatusUpdaterName);
	    } catch (RemoteException e) {
	      e.printStackTrace();
	    } catch (NotBoundException e) {
	      e.printStackTrace();
	    }
		
		run(t.inputPath, t.outputPath, t.codePath);
	}
	
	public abstract void run(String in, String out, String code);
	
	public void updateStatus(){
		try {
			taskStatusUpdater.update(getProgress());
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String getTaskId(){
		return this.taskId;
	}
	
	public TaskProgress getProgress(){
		
		progress.percentage = this.getPercentage();
		
		if(progress.percentage == 100.00)
			progress.status = TaskStatus.SUCCEED;
		else
			progress.status = TaskStatus.INPROGRESS;
		
		return this.progress;
		
	}
	
	public abstract float getPercentage();

}
