import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public abstract class Worker {

  private int taskId;

  private TaskProgress progress;

  private StatusUpdater taskStatusUpdater;

  public Worker(TaskInfo t, String regHostName, int regPort, String taskStatusUpdaterName) {

    this.taskId = t.getTaskID();
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

    run(t.getInputPath(), t.getOutputPath(), t.getCodePath());
  }

  public abstract void run(String in, String out, String code);

  public void updateStatus() {
    try {
      taskStatusUpdater.update(getProgress());
    } catch (RemoteException e) {
      e.printStackTrace();
    }
  }

  public int getTaskId() {
    return this.taskId;
  }

  public TaskProgress getProgress() {
    progress.setPercentage(this.getPercentage());
    progress.setStatus(TaskStatus.INPROGRESS);

    return this.progress;
  }

  public abstract float getPercentage();

}
