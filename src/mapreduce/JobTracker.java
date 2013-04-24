package mapreduce;

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

  public static String JOBTRACKER_SERVICE_NAME;

  public static String JOB_MAPPER_OUTPUT_PREFIX = "mapper_output_job_";

  public static String TASK_MAPPER_OUTPUT_PREFIX = "mapper_output_task_";

  public static int SCHEDULER_POOL_SIZE = 8;

  public static int ALIVE_CHECK_CYCLE_SEC = 4;

  // the directory to which all the classes that user submits are extracted
  public static String JOB_CLASSPATH;

  // the prefix of the directories that stores the classes for different jobs
  public final static String JOB_CLASSPATH_PREFIX = "job";

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

  /*****************************************/

  public JobTracker(String rh, int rp) throws RemoteException {
    this.currentMaxJobId = (int) (Math.random()*1000);
    this.currentMaxTaskId = (int) (Math.random()*1000);

    this.tasktrackers = Collections.synchronizedMap(new HashMap<String, TaskTrackerMeta>());
    this.mapTasks = Collections.synchronizedMap(new HashMap<Integer, TaskMeta>());
    this.reduceTasks = Collections.synchronizedMap(new HashMap<Integer, TaskMeta>());
    this.jobs = Collections.synchronizedMap(new HashMap<Integer, JobMeta>());
    JOBTRACKER_SERVICE_NAME = Utility.getParam("JOB_TRACKER_SERVICE_NAME");
    JOB_CLASSPATH = Utility.getParam("USER_CLASS_PATH");

    // all tasks are queued in a priority queue, which the job id is the priority
    // the smaller the id is, the higher the priority is
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

    // the task schedule of the system
    this.scheduler = new DefaultTaskScheduler(this);

    this.services = new JobTrackerServices(this);

    // register the RMI service this jobtracker provides
    Registry rmiReg = LocateRegistry.getRegistry(rh, rp);
    rmiReg.rebind(JOBTRACKER_SERVICE_NAME, this.services);

    ScheduledExecutorService serviceSche = Executors.newScheduledThreadPool(SCHEDULER_POOL_SIZE);

    // start the task tracker alive checking
    TaskTrackerAliveChecker alivechecker = new TaskTrackerAliveChecker(this);
    serviceSche.scheduleAtFixedRate(alivechecker, ALIVE_CHECK_CYCLE_SEC, ALIVE_CHECK_CYCLE_SEC,
            TimeUnit.SECONDS);

    // the command console to display the statistics of this job tracker
    this.controlConsole();
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

  /**
   * deregister a task tracker
   * 
   * @param ttname
   */
  public void deleteTaskTracker(String ttname) {
    if (ttname == null)
      return;

    if (this.tasktrackers.containsKey(ttname)) {
      this.tasktrackers.remove(ttname);

    }
  }

  /**
   * Retrieve the information of a job
   * 
   * @param jid
   * @return
   */
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

  /**
   * get next mapper task. First, check whether the job, to which this task belongs, has already
   * failed. If so, throw this task; otherwise, return this task.
   * 
   * @return
   */
  public TaskMeta getNextMapperTask() {
    while (!this.mapTasksQueue.isEmpty()) {
      TaskMeta task = this.mapTasksQueue.poll();
      JobMeta job = this.jobs.get(task.getJobID());

      if (job.getStatus() == JobMeta.JobStatus.FAILED) {
        // this job has already failed
        task.getTaskProgress().setStatus(TaskMeta.TaskStatus.FAILED);
      } else {
        return task;
      }
    }

    return null;
  }

  /**
   * get next reducer task. First, check whether the job, to which this task belongs, has already
   * failed. If so, throw this task; otherwise, return this task.
   * 
   * @return
   */
  public TaskMeta getNextReducerTask() {
    while (!this.reduceTasksQueue.isEmpty()) {
      TaskMeta task = this.reduceTasksQueue.poll();
      JobMeta job = this.jobs.get(task.getJobID());

      if (job.getStatus() == JobMeta.JobStatus.FAILED) {
        // this job has already failed;
        task.getTaskProgress().setStatus(TaskMeta.TaskStatus.FAILED);
      } else {
        return task;
      }
    }

    return null;
  }

  /**
   * trigger the task scheduling to fill all those idle slots
   */
  public void distributeTasks() {
    Map<Integer, String> schestrategies = null;

    // use the system's scheduler to generate the scheduling schemes
    synchronized (this.tasktrackers) {
      schestrategies = this.scheduler.scheduleTask();
    }

    if (schestrategies == null)
      return;

    // assign tasks according to the scheduling task
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

      // assign the task to the tasktracker
      boolean res = false;
      try {
        res = targetTasktracker.getTaskLauncher().runTask(task.getTaskInfo());
      } catch (Exception e) {
        res = false;
      }
      if (res) {
        // if this task has been submitted to a tasktracker successfully
        task.getTaskProgress().setStatus(TaskMeta.TaskStatus.INPROGRESS);
      } else {
        // if this task is failed to be submitted, place it back to the queue
        if (task.isMapper()) {
          this.mapTasksQueue.offer(task);
        } else {
          this.reduceTasksQueue.offer(task);
        }
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

      // find the path to which the jar file should be extracted
      String destDirPath = JobTracker.JOB_CLASSPATH + File.separator
              + JobTracker.JOB_CLASSPATH_PREFIX + jobid + File.separator;
      File destDir = new File(destDirPath);
      if (!destDir.exists()) {
        destDir.mkdirs();
      }

      // copy each file in jar archive one by one
      while (enums.hasMoreElements()) {
        JarEntry file = (JarEntry) enums.nextElement();

        File outputfile = new File(destDirPath + file.getName());
        if (file.isDirectory()) {
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

    Map<Integer, TaskMeta> mapTasks = new HashMap<Integer, TaskMeta>();
    Map<Integer, TaskMeta> reduceTasks = new HashMap<Integer, TaskMeta>();

    // create new map tasks for this job
    for (JobMeta.InputBlock block : blocks) {
      int taskid = this.requestTaskId();
      TaskInfo minfo = new MapperTaskInfo(newjob.getJobId(), taskid, block.getFilePath(),
              block.getOffset(), block.getLength(), newjob.getMapperClassName(),
              newjob.getPartitionerClassName(), newjob.getInputFormatClassName(),
              jobMapperOutputDirPath, newjob.getReducerNum());
      TaskMeta mtask = new TaskMeta(taskid, newjob.getJobId(), minfo, new TaskProgress(taskid,
              TaskMeta.TaskType.MAPPER));

      mapTasks.put(taskid, mtask);
      newjob.addMapperTask(taskid);
    }

    // create new reduce tasks for this job
    int reducerNum = newjob.getReducerNum();
    for (int i = 0; i < reducerNum; i++) {
      int taskid = this.requestTaskId();
      TaskInfo rinfo = new ReducerTaskInfo(newjob.getJobId(), taskid, i, jobMapperOutputDirPath,
              newjob.getReducerClassName(), newjob.getOutputFormatClassName(),
              newjob.getOutputPath());
      TaskMeta rtask = new TaskMeta(taskid, newjob.getJobId(), rinfo, new TaskProgress(taskid,
              TaskMeta.TaskType.REDUCER));

      reduceTasks.put(taskid, rtask);
      newjob.addReducerTask(taskid);
    }

    // submit these tasks into the system
    this.mapTasks.putAll(mapTasks);
    this.reduceTasks.putAll(reduceTasks);
    this.mapTasksQueue.addAll(mapTasks.values());
    this.reduceTasksQueue.addAll(reduceTasks.values());

    this.jobs.put(newjob.getJobId(), newjob);
    newjob.setStatus(JobMeta.JobStatus.INPROGRESS);
  }

  /**
   * register a new task by inserting it into a queue
   * 
   * @param task
   */
  public void submitTask(TaskMeta task) {
    if (task.isMapper()) {
      this.mapTasksQueue.offer(task);
    } else {
      this.reduceTasksQueue.offer(task);
    }
  }
  
  public void submitExistingTask(int tid) {
    if (this.mapTasks.containsKey(tid)) {
      this.submitTask(this.mapTasks.get(tid));
    } else {
      this.submitTask(this.reduceTasks.get(tid));
    }
  }

  /**
   * Check the status of the Map phase of a job. This method is used by the reducer to check how is
   * everything going in the mapper phase
   * 
   * @param tid
   *          the task id of the reduce task which send the retrieval request
   * @return
   */
  public MapStatusChecker.MapStatus checkMapStatus(int tid) {
    TaskMeta task = this.reduceTasks.get(tid);

    if (task == null) {
      return MapStatusChecker.MapStatus.INPROGRESS;
    }

    int jid = task.getJobID();

    JobMeta job = this.jobs.get(jid);

    // if the job is failed, then return FAILED
    if (job == null || job.getStatus() == JobMeta.JobStatus.FAILED) {
      return MapStatusChecker.MapStatus.FAILED;
    }

    Set<Integer> mapTasks = job.getMapTasks();
    for (int mid : mapTasks) {
      if (this.mapTasks.containsKey(mid) && !this.mapTasks.get(mid).isDone())
        return MapStatusChecker.MapStatus.INPROGRESS;
    }

    // if all map tasks finished, then return FINISHED
    return MapStatusChecker.MapStatus.FINISHED;
  }

  /**
   * the command line tool to control the job tracker
   */
  public void controlConsole() {
    Scanner scanner = new Scanner(System.in);
    System.out.println(">> ");
    while (scanner.hasNext()) {
      System.out.println(">> ");
      String line = scanner.nextLine().trim();
      String[] fields = line.split(" ");
      String cmd = fields[0];

      if (cmd.compareTo("ls") == 0) {
        switch (fields.length) {
          case 2: // ls job
            if (fields[1].compareTo("job") == 0) {
              this.listAllJobs();
            } else if (fields[1].compareTo("tasktracker") == 0) {
              this.listTaskTrackers();
            } else {
              System.out.println("Invalid command, type 'help' to see the mannul.");
            }
            break;
          case 3:
            if (fields[1].compareTo("job") == 0) {
              try {
                int jid = Integer.parseInt(fields[2]);
                this.listJob(jid);
              } catch (NumberFormatException e) {
                System.out.println("Invalid job id.");
              }
            } else {
              System.out.println("Invalid command, type 'help' to see the mannul.");
            }
            break;
          default:
            break;
        }

      } else if (cmd.compareTo("quit") == 0) {
        System.exit(0);
      } else if (cmd.compareTo("help") == 0) {

      }
    }
  }

  /**
   * list the statistic of all jobs
   */
  private void listAllJobs() {
    System.out.println("========== All Jobs ==========");
    System.out
            .println("JobID\tJobName\tStatus\tTotalTasks\tMapperTasks\tCompleted\tReducerTasks\tCompleted");
    for (JobMeta job : this.jobs.values()) {
      int id = job.getJobId();
      String name = job.getJobName();
      JobMeta.JobStatus status = job.getStatus();
      int mapNum = job.getMapTasks().size();
      int reduceNum = job.getReduceTasks().size();
      int mapfinNum = job.getFinishedMapperNumber();
      int reducefinNum = job.getFinishedReducerNumber();

      System.out.println(id + "\t" + name + "\t" + status + "\t" + (mapNum + reduceNum) + "\t"
              + mapNum + "\t" + mapfinNum + "\t" + reduceNum + "\t" + reducefinNum);
    }
  }

  /**
   * list the statistic of a specific job
   * 
   * @param jid
   */
  private void listJob(int jid) {
    if (!this.jobs.containsKey(jid)) {
      System.out.println("Job " + jid + " does not exist.");
      return;
    }

    JobMeta job = this.jobs.get(jid);
    System.out.println("========== Job " + jid + " ==========");

    System.out.println("Name: " + job.getJobName());
    System.out.println("Status: " + job.getStatus());
    System.out.println("Number of Mapper Tasks (finished/total): " + job.getFinishedMapperNumber()
            + "/" + job.getMapTasks().size());
    System.out.println("Number of Reducer Tasks (finished/total): "
            + job.getFinishedReducerNumber() + "/" + job.getReduceTasks().size());
    System.out.println("\nMap Tasks:");
    Set<Integer> maps = job.getMapTasks();
    System.out.println("TaskID\tAttempts\tTaskStatus");
    for (int tid : maps) {
      TaskMeta task = this.mapTasks.get(tid);
      System.out.println(tid + "\t" + task.getAttempts() + "\t"
              + task.getTaskProgress().getStatus());
    }
    System.out.println("\nReduce Tasks:");
    System.out.println("TaskID\tAttempts\tTaskStatus");
    Set<Integer> reduces = job.getReduceTasks();
    for (int tid : reduces) {
      TaskMeta task = this.reduceTasks.get(tid);
      System.out.println(tid + "\t" + task.getAttempts() + "\t"
              + task.getTaskProgress().getStatus());
    }
  }

  /**
   * list all task trackers connected to current job tracker
   */
  private void listTaskTrackers() {
    System.out.println("========== Task Trackers ==========");
    System.out.println("Name\tAvailableMapperSlots\tAvailableReducerSlots");
    for (TaskTrackerMeta tt : this.tasktrackers.values()) {
      System.out.println(tt.getTaskTrackerName() + "\t" + tt.getNumOfMapperSlots() + "\t"
              + tt.getNumOfReducerSlots());
    }
  }

  /**
   * get the system's temporary dir which holds mapper's output
   * 
   * @return
   */
  public static String getSystemTempDir() {
    String res = Utility.getParam("SYSTEM_TEMP_DIR");
    if (res.compareTo("") == 0)
      res = System.getProperty("java.io.tmpdir");

    File tmpdir = new File(res);
    if (!tmpdir.exists()) {
      tmpdir.mkdirs();
    }

    return res;
  }

  public static void main(String[] args) {
    try {
      JobTracker jb = new JobTracker(Utility.getParam("JOB_TRACKER_REGISTRY_HOST"),
              Integer.parseInt(Utility.getParam("REGISTRY_PORT")));
    } catch (RemoteException e) {
      e.printStackTrace();
    }
  }

}
