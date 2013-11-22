package mapreduce;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 
 * This class is used to run mapper tasks or reducer task
 * 
 */
public abstract class Worker {

  /* the id of the task */
  protected int taskID;

  /* the output path */
  protected String outputFile;

  /* the input path */
  protected String inputFile;

  /* the service used to update task status to task tracker */
  protected StatusUpdater taskStatusUpdater;

  /* the progress of the task, including percentage, failed, succeed, or in progress etc */
  protected TaskProgress progress;

  /**
   * constructor method
   * 
   * @param taskID
   * @param infile
   * @param outfile
   * @param taskTrackerServiceName
   * @param type
   */
  public Worker(int taskID, String infile, String outfile, String taskTrackerServiceName,
          TaskMeta.TaskType type, int rPort) {

    this.taskID = taskID;
    this.outputFile = outfile;
    this.inputFile = infile;
    this.progress = new TaskProgress(this.taskID, type);

    /* get the task tracker status updater from rmi */
    String registryHostName = null;
    try {
      registryHostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e1) {
      e1.printStackTrace();
    }

    try {
      Registry reg = LocateRegistry.getRegistry(registryHostName, rPort);
      taskStatusUpdater = (StatusUpdater) reg.lookup(taskTrackerServiceName);
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (NotBoundException e) {
      e.printStackTrace();
    }

  }

  /**
   * abstract method used to run task
   */
  public abstract void run();

  /**
   * periodically update task status to task tracker
   */
  public void updateStatusToTaskTracker() {
    /* periodically send status progress to task tracker */
    int poolSize = Integer.parseInt(Utility.getParam("THREAD_POOL_SIZE"));
    ScheduledExecutorService schExec = Executors.newScheduledThreadPool(poolSize);
    Thread thread = new Thread(new Runnable() {
      public void run() {
        updateStatus();
      }
    });
    thread.setDaemon(true);
    schExec.scheduleAtFixedRate(thread, 0, Integer.parseInt(Utility.getParam("HEART_BEAT_PERIOD")),
            TimeUnit.SECONDS);
  }

  /**
   * update in-progress status to task tracker
   */
  public void updateStatus() {
    /* need to change progress, so first lock it */
    synchronized (progress) {
      /* if already succeed, stop. if not, send in-progress status */
      if (progress.getStatus() != TaskMeta.TaskStatus.SUCCEED) {
        try {
          /* set percentage of work already done */
          progress.setPercentage(this.getPercentage());

          /* set status as in-progress */
          progress.setStatus(TaskMeta.TaskStatus.INPROGRESS);

          /* set the current time stamp, for task tacker to detect if alive */
          progress.setTimestamp(System.currentTimeMillis());

          /* update to task tracker */
          taskStatusUpdater.update(progress);

        } catch (RemoteException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * update succeed status to task tracker only used when the task is done
   */
  public void updateStatusSucceed() {
    /* lock progress before change it */
    synchronized (progress) {
      try {
        /* set percentage */
        progress.setPercentage(this.getPercentage());

        /* set status as succeed */
        progress.setStatus(TaskMeta.TaskStatus.SUCCEED);

        /* set the current time stamp */
        progress.setTimestamp(System.currentTimeMillis());

        /* update to task tracker */
        taskStatusUpdater.update(progress);
      } catch (RemoteException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * @return percentage of work already done
   */
  protected abstract float getPercentage();

}
