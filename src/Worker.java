import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract class Worker {

  protected int taskID;

  protected String outputFile;
  
  protected String inputFile;

  protected StatusUpdater taskStatusUpdater;

  protected TaskProgress progress;

  public Worker(int taskID, String infile, String outfile, String taskTrackerServiceName) {

    this.taskID = taskID;
    this.outputFile = outfile;
    this.inputFile = infile;
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

  public void updateStatusToTaskTracker() {
    /* periodically send status progress to job tracker */
    ScheduledExecutorService schExec = Executors.newScheduledThreadPool(8);
    ScheduledFuture<?> schFuture = schExec.scheduleAtFixedRate(new Runnable() {
      public void run() {
        updateStatus();
      }
    }, 0, Integer.parseInt(Utility.getParam("HEART_BEAT_PERIOD")), TimeUnit.SECONDS);
  }

  public void updateStatus() {
    try {
      progress.setPercentage(this.getPercentage());
      progress.setStatus(TaskStatus.INPROGRESS);
      progress.setTimestamp(System.currentTimeMillis());
      taskStatusUpdater.update(progress);
    } catch (RemoteException e) {
      e.printStackTrace();
    }
  }

  public void updateStatusSucceed() {
    try {
      progress.setPercentage(this.getPercentage());
      progress.setStatus(TaskStatus.SUCCEED);
      progress.setTimestamp(System.currentTimeMillis());
      taskStatusUpdater.update(progress);
    } catch (RemoteException e) {
      e.printStackTrace();
    }
  }

  protected abstract float getPercentage();

}
