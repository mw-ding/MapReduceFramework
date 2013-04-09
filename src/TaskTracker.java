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

public class TaskTracker implements Runnable {
  private String registryHostName;

  private int registryPort;

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
  public TaskTracker(String rHostName, int rPort, String taskTrackerName, int numOfMapperSlots,
          int numOfReducerSlots, String jobTrackerStatusUpdaterName) {
    this.registryHostName = rHostName;
    this.registryPort = rPort;
    this.taskTrackerName = taskTrackerName;
    this.mapperCounter = new AtomicInteger();
    this.reducerCounter = new AtomicInteger();

    this.NUM_OF_MAPPER_SLOTS = numOfMapperSlots;
    this.NUM_OF_REDUCER_SLOTS = numOfReducerSlots;

    /* get the job tracker status updater */
    try {
      Registry reg = LocateRegistry.getRegistry(this.registryHostName, this.registryPort);
      jobTrackerStatusUpdater = (StatusUpdater) reg.lookup(jobTrackerStatusUpdaterName);
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (NotBoundException e) {
      e.printStackTrace();
    }

    /* initiate task status */
    this.taskStatus = new HashMap<Integer, TaskProgress>();

    this.registerServices();

  }

  /**
   * register services to rmi registry
   */
  private void registerServices() {
    try {
      TaskTrackerServices tts = new TaskTrackerServices(this);
      Registry reg = LocateRegistry.getRegistry(this.registryHostName, this.registryPort);
      reg.bind(this.taskTrackerName, tts);
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (AlreadyBoundException e) {
      e.printStackTrace();
    }
  }

  public TaskOutput runTask(TaskInfo taskinfo) {
    return null;
  }

  public Map<Integer, TaskProgress> getTaskStatus() {
    return taskStatus;
  }

  @Override
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
        }
        TaskTrackerUpdatePkg pkg = new TaskTrackerUpdatePkg(taskTrackerName, NUM_OF_MAPPER_SLOTS
                - mapperCounter.get(), NUM_OF_REDUCER_SLOTS - reducerCounter.get(), taskList);
        try {
          jobTrackerStatusUpdater.update(pkg);
        } catch (RemoteException e) {
          e.printStackTrace();
        }
      }
    }, 0, HEART_BEAT_PERIOD, TimeUnit.SECONDS);
  }

  public String getRegistryHostName() {
    return registryHostName;
  }

  public void setRegistryHostName(String registryHostName) {
    this.registryHostName = registryHostName;
  }

  public int getRegistryPort() {
    return registryPort;
  }

  public void setRegistryPort(int registryPort) {
    this.registryPort = registryPort;
  }

  public String getTaskTrackerName() {
    return taskTrackerName;
  }

  public void setTaskTrackerName(String taskTrackerName) {
    this.taskTrackerName = taskTrackerName;
  }

}
