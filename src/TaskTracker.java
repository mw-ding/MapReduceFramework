import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskTracker {

  public static String TASKTRACKER_SERVICE_NAME;

  private String taskTrackerName;

  public AtomicInteger mapperCounter;

  public AtomicInteger reducerCounter;

  private StatusUpdater jobTrackerStatusUpdater;

  private HashMap<Integer, TaskProgress> taskStatus;

  /* period between heart beat in seconds */
  private final int HEART_BEAT_PERIOD = 2;

  /* number of mapper slots */
  public final int NUM_OF_MAPPER_SLOTS;

  /* number of reducer slots */
  public final int NUM_OF_REDUCER_SLOTS;

  /**
   * constructor of task tracker
   * 
   * @param rHostName
   * @param rPort
   * @param taskTrackerName
   * @param jobTrackerStatusUpdaterName
   */
  public TaskTracker(int taskTrackerSeq) {
    this.taskTrackerName = Utility.getParam("TASK_TRACKER_" + taskTrackerSeq + "_NAME");
    System.out.println(taskTrackerName + " STARTED..");
    this.NUM_OF_MAPPER_SLOTS = Integer.parseInt(Utility.getParam("TASK_TRACKER_" + taskTrackerSeq
            + "_NUM_MAPPER"));
    this.NUM_OF_REDUCER_SLOTS = Integer.parseInt(Utility.getParam("TASK_TRACKER_" + taskTrackerSeq
            + "_NUM_REDUCER"));
    this.mapperCounter = new AtomicInteger();
    this.reducerCounter = new AtomicInteger();
    /* initiate task status */
    this.taskStatus = new HashMap<Integer, TaskProgress>();

    /* get the registry information */
    String registryHostName = Utility.getParam("REGISTRY_HOST");
    int registryPort = Integer.parseInt(Utility.getParam("REGISTRY_PORT"));

    /* get the job tracker status updater */
    try {
      Registry reg = LocateRegistry.getRegistry(registryHostName, registryPort);
      jobTrackerStatusUpdater = (StatusUpdater) reg.lookup(Utility
              .getParam("JOB_TRACKER_SERVICE_NAME"));
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (NotBoundException e) {
      System.err.println(Utility.getParam("JOB_TRACKER_SERVICE_NAME") + " is not registered.");
    }

    /* register service to registry */
    try {
      TaskTrackerServices tts = new TaskTrackerServices(this);
      Registry reg = LocateRegistry.getRegistry(registryHostName, registryPort);
      TaskTracker.TASKTRACKER_SERVICE_NAME = this.taskTrackerName;
      reg.rebind(TaskTracker.TASKTRACKER_SERVICE_NAME, tts);
    } catch (RemoteException e) {
      e.printStackTrace();
    }

  }

  public TaskOutput runTask(TaskInfo taskinfo) {
    return null;
  }

  public Map<Integer, TaskProgress> getTaskStatus() {
    return taskStatus;
  }

  public void run() {
    /* start the task status checker */
    Thread taskStatusChecker = new Thread(new TaskStatusChecker(this));
    taskStatusChecker.start();
    /* periodically send status progress to job tracker */
    ScheduledExecutorService schExec = Executors.newScheduledThreadPool(8);
    ScheduledFuture<?> schFuture = schExec.scheduleAtFixedRate(new Runnable() {
      public void run() {
        ArrayList<TaskProgress> taskList;
        synchronized (taskStatus) {
          taskList = new ArrayList<TaskProgress>(taskStatus.values());
          /* delete the job that has failed or succeeded */
          ArrayList<Integer> toDelete = new ArrayList<Integer>();
          for (int id : taskStatus.keySet()) {
            if (taskStatus.get(id).getStatus() != TaskStatus.INPROGRESS)
              toDelete.add(id);
            /* free slots */
            if (taskStatus.get(id).getType() == TaskType.MAPPER
                    && taskStatus.get(id).getStatus() == TaskStatus.SUCCEED)
              mapperCounter.decrementAndGet();
            else
              reducerCounter.decrementAndGet();
          }
          for (int id : toDelete) {
            taskStatus.remove(id);
          }
          /* delete done */
        }
        TaskTrackerUpdatePkg pkg = new TaskTrackerUpdatePkg(taskTrackerName, NUM_OF_MAPPER_SLOTS
                - mapperCounter.get(), NUM_OF_REDUCER_SLOTS - reducerCounter.get(),
                TaskTracker.TASKTRACKER_SERVICE_NAME, taskList);
        try {
          jobTrackerStatusUpdater.update(pkg);
        } catch (RemoteException e) {
          e.printStackTrace();
        }
      }
    }, 0, HEART_BEAT_PERIOD, TimeUnit.SECONDS);
  }

  public String getTaskTrackerName() {
    return taskTrackerName;
  }

  public void setTaskTrackerName(String taskTrackerName) {
    this.taskTrackerName = taskTrackerName;
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("illegal arguments");
      return;
    }
    TaskTracker tt = new TaskTracker(Integer.parseInt(args[0]));
    tt.run();
  }

}
