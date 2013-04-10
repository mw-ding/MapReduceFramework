import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public abstract class Worker {

  private int taskID;

  String outputFile;

  String code;

  private StatusUpdater taskStatusUpdater;

  private TaskProgress progress;

  public Worker(int taskID, String outputFile, String code, String taskTrackerServiceName) {

    this.taskID = taskID;
    this.outputFile = outputFile;
    this.code = code;
    this.progress = new TaskProgress(this.taskID);

    /* get the task tracker status updater */
    String registryHostName = Utility.getParam("REGISTRY_HOST");
    int registryPort = Integer.parseInt(Utility.getParam("REGISTRY_PORT"));

    try {
      Registry reg = LocateRegistry.getRegistry(registryHostName, registryPort);
      taskStatusUpdater = (StatusUpdater) reg.lookup(taskTrackerServiceName);
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (NotBoundException e) {
      e.printStackTrace();
    }

  }

  public abstract void run();

  public void updateStatus() {
    try {
      taskStatusUpdater.update(getProgress());
    } catch (RemoteException e) {
      e.printStackTrace();
    }
  }

  public TaskProgress getProgress() {
    progress.setPercentage(this.getPercentage());
    progress.setStatus(TaskStatus.INPROGRESS);
    progress.setTimestamp(System.currentTimeMillis());
    return this.progress;
  }

  public abstract float getPercentage();

}
