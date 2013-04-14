package mapreduce;

import java.net.InetAddress;
import java.net.UnknownHostException;
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

/**
 * task tracker
 * 
 */
public class TaskTracker {

  /* the service name provided by the task tracker */
  public static String TASKTRACKER_SERVICE_NAME;

  /* the name of the task tracker */
  private String taskTrackerName;

  /* the numebr of running mappers */
  public Integer mapperCounter;

  /* the number of running reducers */
  public Integer reducerCounter;

  /* the service from job tracker, used to */
  private StatusUpdater jobTrackerStatusUpdater;

  private HashMap<Integer, TaskProgress> taskStatus;

  /* period between heart beat in seconds */
  private final int HEART_BEAT_PERIOD = 2;

  /* number of mapper slots */
  public final int NUM_OF_MAPPER_SLOTS;

  /* number of reducer slots */
  public final int NUM_OF_REDUCER_SLOTS;

  /* the registry port of this task tracker */
  private int rPort;

  /**
   * constructor of task tracker
   * 
   */
  public TaskTracker(int taskTrackerSeq) {
    this.taskTrackerName = Utility.getParam("TASK_TRACKER_" + taskTrackerSeq + "_NAME");
    System.out.println(taskTrackerName + " STARTED..");
    this.NUM_OF_MAPPER_SLOTS = Integer.parseInt(Utility.getParam("TASK_TRACKER_" + taskTrackerSeq
            + "_NUM_MAPPER"));
    this.NUM_OF_REDUCER_SLOTS = Integer.parseInt(Utility.getParam("TASK_TRACKER_" + taskTrackerSeq
            + "_NUM_REDUCER"));
    this.mapperCounter = new Integer(0);
    this.reducerCounter = new Integer(0);
    /* initiate task status */
    this.taskStatus = new HashMap<Integer, TaskProgress>();

    /* get the job tracker registry information */
    String registryHostName = Utility.getParam("JOB_TRACKER_REGISTRY_HOST");

    int registryPort = Integer.parseInt(Utility.getParam("REGISTRY_PORT"));

    /* all registries are using the same port number */
    this.rPort = registryPort;

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
      String rHostName = null;
      try {
        rHostName = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      Registry reg = LocateRegistry.getRegistry(rHostName, registryPort);
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
    taskStatusChecker.setDaemon(true);
    Thread taskStatusUpdater = new Thread(new Runnable() {
      public void run() {
        ArrayList<TaskProgress> taskList;
        synchronized (taskStatus) {
          taskList = new ArrayList<TaskProgress>(taskStatus.values());
          /* delete the job that has failed or succeeded */
          ArrayList<Integer> toDelete = new ArrayList<Integer>();
          for (int id : taskStatus.keySet()) {
            if (taskStatus.get(id).getStatus() != TaskMeta.TaskStatus.INPROGRESS) {
              toDelete.add(id);

              /* free slots */
              if (taskStatus.get(id).getType() == TaskMeta.TaskType.MAPPER)
                synchronized (mapperCounter) {
                  mapperCounter--;
                }
              else
                synchronized (reducerCounter) {
                  reducerCounter--;
                }
            }
          }
          /* delete task status */
          for (int id : toDelete) {
            taskStatus.remove(id);
          }
          /* delete done */
          /* build update package */
          TaskTrackerUpdatePkg pkg = null;
          synchronized (mapperCounter) {
            synchronized (reducerCounter) {
              pkg = new TaskTrackerUpdatePkg(taskTrackerName, NUM_OF_MAPPER_SLOTS - mapperCounter,
                      NUM_OF_REDUCER_SLOTS - reducerCounter, TaskTracker.TASKTRACKER_SERVICE_NAME,
                      taskList, rPort);
            }
          }
          /* send update package */
          if (pkg != null)
            try {
              jobTrackerStatusUpdater.update(pkg);
            } catch (RemoteException e) {
              e.printStackTrace();
            }
        }
      }
    });
    taskStatusUpdater.setDaemon(true);
    /* periodically send status progress to job tracker */
    int poolSize = Integer.parseInt(Utility.getParam("THREAD_POOL_SIZE"));
    ScheduledExecutorService schExec = Executors.newScheduledThreadPool(poolSize);
    ScheduledFuture<?> schFutureChecker = schExec.scheduleAtFixedRate(taskStatusChecker, 0,
            HEART_BEAT_PERIOD, TimeUnit.SECONDS);
    ScheduledFuture<?> schFutureUpdater = schExec.scheduleAtFixedRate(taskStatusUpdater, 0,
            HEART_BEAT_PERIOD, TimeUnit.SECONDS);
  }

  public String getTaskTrackerName() {
    return taskTrackerName;
  }

  public void setTaskTrackerName(String taskTrackerName) {
    this.taskTrackerName = taskTrackerName;
  }

  public int getRPort() {
    return this.rPort;
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
