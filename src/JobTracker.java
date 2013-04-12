import java.io.*;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class JobTracker {

  public final static String JOBTRACKER_SERVICE_NAME = "job_tracker_service";

  public final static String JOB_MAPPER_OUTPUT_PREFIX = "mapper_output_job_";
  
  public final static String TASK_MAPPER_OUTPUT_PREFIX = "mapper_output_task_";

  // all the tasktrackers running under current system
  private Map<String, TaskTrackerMeta> tasktrackers;

  // all the map tasks, including accomplished, executing, and uninitiated
  private Map<Integer, TaskMeta> mapTasks;

  // all the reduce tasks, including accomplished, executing, and uninitiated
  private Map<Integer, TaskMeta> reduceTasks;

  // all the jobs, including accomplished, executing, and uninitiated
  private Map<Integer, JobMeta> jobs;

  // the map task queue that all map tasks that have not been arranged to execute
  private Queue<TaskMeta> mapTasksQueue;

  // the reduce task queue that all reduce tasks that have not been arranged to execute
  private Queue<TaskMeta> reduceTasksQueue;

  // all the communication services that this jobtracker provides
  private JobTrackerServices services;

  // the scheduler, which generally decides assign which map/reduce task of
  // which job to which tasktracker.
  private TaskScheduler scheduler;

  // the maximum assigned job id currently
  private int currentMaxJobId;

  // the maximum assigned task id currently
  private int currentMaxTaskId;

  private Registry rmiReg;

  public final static int SCHEDULER_POOL_SIZE = 8;

  public final static int ALIVE_CHECK_CYCLE_SEC = 4;

  // the directory to which all the classes that user submits are extracted
  public final static String JOB_CLASSPATH = "bin" + File.separator;

  // the prefix of the directories that stores the classes for different jobs
  public final static String JOB_CLASSPATH_PREFIX = "job";

  /*****************************************/

  public JobTracker(String rh, int rp) throws RemoteException {
    this.currentMaxJobId = 0;
    this.currentMaxTaskId = 0;

    this.tasktrackers = Collections.synchronizedMap(new HashMap<String, TaskTrackerMeta>());
    this.mapTasks = Collections.synchronizedMap(new HashMap<Integer, TaskMeta>());
    this.reduceTasks = Collections.synchronizedMap(new HashMap<Integer, TaskMeta>());
    this.jobs = Collections.synchronizedMap(new HashMap<Integer, JobMeta>());

    this.mapTasksQueue = (Queue<TaskMeta>) (new PriorityQueue<TaskMeta>(10,
            new Comparator<TaskMeta>() {

              @Override
              public int compare(TaskMeta o1, TaskMeta o2) {
                return o1.getJobID() - o2.getJobID();
              }

            }));

    this.reduceTasksQueue = (Queue<TaskMeta>) (new PriorityQueue<TaskMeta>(10,
            new Comparator<TaskMeta>() {

              @Override
              public int compare(TaskMeta o1, TaskMeta o2) {
                return o1.getJobID() - o2.getJobID();
              }

            }));

    this.scheduler = new DefaultTaskScheduler(this.tasktrackers, this.mapTasksQueue,
            this.reduceTasksQueue);

    this.services = new JobTrackerServices(this);
    this.rmiReg = LocateRegistry.getRegistry(rh, rp);
    this.rmiReg.rebind(JOBTRACKER_SERVICE_NAME, this.services);

    ScheduledExecutorService serviceSche = Executors.newScheduledThreadPool(SCHEDULER_POOL_SIZE);

    // start the task tracker alive checking
    TaskTrackerAliveChecker alivechecker = new TaskTrackerAliveChecker(this);
    serviceSche.scheduleAtFixedRate(alivechecker, ALIVE_CHECK_CYCLE_SEC, ALIVE_CHECK_CYCLE_SEC,
            TimeUnit.SECONDS);
  }

  /**
   * Change the task scheduler at runtime
   * 
   * @param s
   *          a new scheduler
   */
  public void setScheduler(TaskScheduler s) {
    this.scheduler = s;
  }

  /**
   * Register a new tasktracker in this jobtracker
   * 
   * @param tt
   *          the metadata of this trasktracker
   */
  public boolean registerTaskTracker(TaskTrackerMeta tt) {
    if (tt == null)
      return false;

    if (this.tasktrackers.containsKey(tt.getTaskTrackerName())) {
      System.err.println("The TaskTracker \"" + tt.getTaskTrackerName() + "\" already exist.");
      return false;
    } else {
      System.err.println("Register a new tasktracker : " + tt.getTaskTrackerName());
      this.tasktrackers.put(tt.getTaskTrackerName(), tt);
      return true;
    }
  }

  /**
   * get the whole list of task trackers
   * 
   * @return
   */
  public Map<String, TaskTrackerMeta> getTaskTrackers() {
    return Collections.unmodifiableMap(this.tasktrackers);
  }

  /**
   * retrieve a specific task tracker
   * 
   * @param id
   *          the id of the task tracker
   * @return the task tracker if it exist; null if not
   */
  public TaskTrackerMeta getTaskTracker(String id) {
    if (this.tasktrackers.containsKey(id)) {
      return this.tasktrackers.get(id);
    } else {
      return null;
    }
  }

  public void deleteTaskTracker(String ttname) {
    if (ttname == null)
      return;

    if (this.tasktrackers.containsKey(ttname)) {
      this.tasktrackers.remove(ttname);

    }
  }

  public JobMeta getJob(int jid) {
    return this.jobs.get(jid);
  }

  /**
   * get the map tasks list
   * 
   * @return
   */
  public Map<Integer, TaskMeta> getMapTasks() {
    return Collections.unmodifiableMap(this.mapTasks);
  }

  /**
   * get the reduce tasks list
   * 
   * @return
   */
  public Map<Integer, TaskMeta> getReduceTasks() {
    return Collections.unmodifiableMap(this.reduceTasks);
  }

  /**
   * get next available job id
   * 
   * @return
   */
  public int requestJobId() {
    int result = (++this.currentMaxJobId);
    return result;
  }

  /**
   * get next available task id
   * 
   * @return
   */
  public int requestTaskId() {
    int result = (++this.currentMaxTaskId);
    return result;
  }

  public Registry getRMIRegistry() {
    return rmiReg;
  }

  /**
   * trigger the task scheduling to fill all those idle slots
   */
  public void distributeTasks() {
    System.out.println("Distributing tasks.");
    Map<Integer, String> schestrategies = null;
    synchronized (this.tasktrackers) {
      schestrategies = this.scheduler.scheduleTask();
    }

    if (schestrategies == null)
      return;

    for (Entry<Integer, String> entry : schestrategies.entrySet()) {
      Integer taskid = entry.getKey();

      // find the task meta data
      TaskMeta task = null;

      if (this.mapTasks.containsKey(taskid)) {
        task = this.mapTasks.get(taskid);
      }

      if (this.reduceTasks.containsKey(taskid)) {
        task = this.reduceTasks.get(taskid);
      }

      if (task == null)
        continue;

      // find the specific task tracker
      TaskTrackerMeta targetTasktracker = this.tasktrackers.get(entry.getValue());

      try {
        // assign the task to the tasktracker
        boolean res = targetTasktracker.getTaskLauncher().runTask(task.getTaskInfo());

        if (res) {
          // if this task has been submitted to a tasktracker successfully
          System.out.println("Assign task " + task.getTaskID() + " to tasktracker "
                  + targetTasktracker.getTaskTrackerName());
          task.getTaskProgress().setStatus(TaskStatus.INPROGRESS);
        } else {
          // if this task is failed to be submitted, place it back to the queue
          if (task.isMapper()) {
            this.mapTasksQueue.offer(task);
          } else {
            this.reduceTasksQueue.offer(task);
          }
        }
      } catch (RemoteException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * extract the jar file into the system's ClassPath folder
   * 
   * @param jobid
   * @param jarpath
   * @return
   */
  public boolean extractJobClassJar(int jobid, String jarpath) {
    try {
      JarFile jar = new JarFile(jarpath);
      Enumeration enums = jar.entries();
      String destDirPath = JobTracker.JOB_CLASSPATH + JobTracker.JOB_CLASSPATH_PREFIX + jobid
              + File.separator;

      System.out.println("Extracting file to " + destDirPath);

      // copy each file in jar archive one by one
      while (enums.hasMoreElements()) {
        JarEntry file = (JarEntry) enums.nextElement();

        File outputfile = new File(destDirPath + file.getName());
        if (outputfile.isDirectory()) {
          outputfile.mkdirs();
          continue;
        }

        InputStream is = jar.getInputStream(file);
        FileOutputStream fos = null;
        try {
          fos = new FileOutputStream(outputfile);
        } catch (FileNotFoundException e) {
          outputfile.getParentFile().mkdirs();
          fos = new FileOutputStream(outputfile);
        }

        while (is.available() > 0) {
          fos.write(is.read());
        }

        fos.close();
        is.close();
      }
    } catch (IOException e) {
      // TODO : handle this exception if the jar file cannot be found
      return false;
    }

    return true;
  }

  /**
   * submit a new job
   * 
   * @param meta
   */
  public void submitJob(JobMeta newjob) {
    // split the input
    newjob.splitInput();
    List<JobMeta.InputBlock> blocks = newjob.getInputBlocks();

    // prepare the temporary output dir of mapper for this job
    String jobMapperOutputDirPath = this.getSystemTempDir() + File.separator
            + JobTracker.JOB_MAPPER_OUTPUT_PREFIX + newjob.getJobId();
    (new File(jobMapperOutputDirPath)).mkdir();

    System.out.println("get " + blocks.size() + " splits.");

    Map<Integer, TaskMeta> mapTasks = new HashMap<Integer, TaskMeta>();
    Map<Integer, TaskMeta> reduceTasks = new HashMap<Integer, TaskMeta>();

    // create new map tasks for this job
    for (JobMeta.InputBlock block : blocks) {
      int taskid = this.requestTaskId();
      TaskInfo minfo = new MapperTaskInfo(taskid, block.getFilePath(), block.getOffset(),
              block.getLength(), newjob.getMapperClassName(), newjob.getPartitionerClassName(),
              newjob.getInputFormatClassName(), jobMapperOutputDirPath, newjob.getReducerNum());
      TaskMeta mtask = new TaskMeta(taskid, newjob.getJobId(), minfo, new TaskProgress(taskid, TaskType.MAPPER));

      mapTasks.put(taskid, mtask);
      newjob.addMapperTask(taskid);
    }

    // create new reduce tasks for this job
    int reducerNum = newjob.getReducerNum();
    for (int i = 0; i < reducerNum; i++) {
      int taskid = this.requestTaskId();
      TaskInfo rinfo = new ReducerTaskInfo(taskid, i, jobMapperOutputDirPath, newjob.getReducerClassName(),
              newjob.getOutputFormatClassName(), newjob.getOutputPath());
      TaskMeta rtask = new TaskMeta(taskid, newjob.getJobId(), rinfo, new TaskProgress(taskid, TaskType.REDUCER));

      reduceTasks.put(taskid, rtask);
      newjob.addReducerTask(taskid);
    }

    System.out.println("Add " + mapTasks.size() + " mapper tasks.");
    System.out.println("Add " + reduceTasks.size() + " reducer tasks.");

    // submit these tasks into the system
    this.mapTasks.putAll(mapTasks);
    this.reduceTasks.putAll(reduceTasks);
    this.mapTasksQueue.addAll(mapTasks.values());
    this.reduceTasksQueue.addAll(reduceTasks.values());

    this.jobs.put(newjob.getJobId(), newjob);
    newjob.setStatus(JobMeta.JobStatus.INPROGRESS);
  }

  public void submitTask(TaskMeta task) {
    if (task.isMapper()) {
      this.mapTasksQueue.offer(task);
    } else {
      this.reduceTasksQueue.offer(task);
    }
  }
  
  public boolean isAllMapperFinished(int tid) {
    TaskMeta task = this.reduceTasks.get(tid);
    
    if (task == null) {
      return false;
    }
    
    int jid = task.getJobID();
    
    JobMeta job = this.jobs.get(jid);
    
    if (job == null) {
      return false;
    }
    
    Set<Integer> mapTasks = job.getMapTasks();
    for (int mid : mapTasks) {
      if (this.mapTasks.containsKey(mid) && !this.mapTasks.get(mid).isDone())
        return false;
    }
    
    return true;
  }

  public static void main(String[] args) {
    try {
      JobTracker jb = new JobTracker("127.0.0.1", 12345);
    } catch (RemoteException e) {
      e.printStackTrace();
    }
  }

  public static String getSystemTempDir() {
    
    File tmp = new File("tmp");
    if (!tmp.exists()) {
      tmp.mkdir();
    }
    
    return "tmp";
  }

}
