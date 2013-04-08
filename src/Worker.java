import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public abstract class Worker {

	private String taskId;
	private String taskStatus;
	private StatusUpdater taskStatusUpdater;
	
	
	public Worker(TaskInfo t, String regHostName, int regPort, String taskStatusUpdaterName){
		
		this.taskStatus = "0";
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
			taskStatusUpdater.update(this.taskStatus);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String getTaskId(){
		return this.taskId;
	}

}
