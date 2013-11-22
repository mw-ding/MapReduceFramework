package mapreduce;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;

/**
 * This class provides all the RMI services for remote procedure call. Generally
 * speaking, this is the communication module for job tracker
 */
public class JobTrackerServices extends UnicastRemoteObject implements StatusUpdater,
        JobTrackerJobSubmitter, MapStatusChecker {

  private JobTracker jobTracker;

  protected JobTrackerServices(JobTracker jt) throws RemoteException {
    super();
    this.jobTracker = jt;
  }

  /**
   * The method is called remotely by each task tracker to send their
   * update information. This could be regarded as heartbeat.
   */
  @Override
  public void update(Object statusPkg) throws RemoteException {

    /* check update package class */
    if (statusPkg.getClass().getName().compareTo(TaskTrackerUpdatePkg.class.getName()) != 0)
      return;

    TaskTrackerUpdatePkg taskTrackerPkg = (TaskTrackerUpdatePkg) statusPkg;

    /* update the current taskTracker status */
    String taskName = taskTrackerPkg.getTaskTrackerName();
    TaskTrackerMeta ttmeta = this.jobTracker.getTaskTracker(taskName);

    if (ttmeta == null) {
      // register this tasktracker first
      try {
        String rhost = taskTrackerPkg.getRmiHostName();
        int rport = taskTrackerPkg.getRmiPort();
        Registry rmiReg = LocateRegistry.getRegistry(rhost, rport);
        
        TaskLauncher taskLauncher = (TaskLauncher) rmiReg.lookup(taskTrackerPkg.getServiceName());
        ttmeta = new TaskTrackerMeta(taskTrackerPkg.getTaskTrackerName(), taskLauncher);
        if (this.jobTracker.registerTaskTracker(ttmeta)) {
          System.err.println("Register successfully");
        } else {
          System.err.println("Register failed");
          return;
        }
      } catch (NotBoundException e) {
        System.out.println("Cannot retrieve the service ");
        return;
      }
    }

    /* update slot and timestamp information */
    ttmeta.setNumOfMapperSlots(taskTrackerPkg.getNumOfMapperSlots());
    ttmeta.setNumOfReducerSlots(taskTrackerPkg.getNumOfReducerSlots());
    ttmeta.setTimestamp(System.currentTimeMillis());

    /* update the tasks the taskTracker maintains */
    this.updateTaskStatus(taskTrackerPkg);
  }

  /**
   * the method to update the status of all involved tasks in a task tracker
   * @param taskTrackerPkg
   */
  private void updateTaskStatus(TaskTrackerUpdatePkg taskTrackerPkg) {
    List<TaskProgress> taskStatus = taskTrackerPkg.getTaskStatus();
    Map<Integer, TaskMeta> allMapTasks = this.jobTracker.getMapTasks();
    Map<Integer, TaskMeta> allReduceTasks = this.jobTracker.getReduceTasks();

    for (TaskProgress taskProg : taskStatus) {
      int taskid = taskProg.getTaskID();
      TaskMeta task = null;

      /* check whether task exist */
      if (allMapTasks.containsKey(taskid)) {
        task = allMapTasks.get(taskid);
      } else if (allReduceTasks.containsKey(taskid)) {
        task = allReduceTasks.get(taskid);
      }

      if (task == null) {
        System.err.println("Task " + taskid + " does not exist.");
        continue;
      }

      task.setTaskProgress(taskProg);
      /* do handling when a task finishes */
      if (taskProg.getStatus() == TaskMeta.TaskStatus.SUCCEED) {
        // 1. remove the task from the tasktracker
        this.jobTracker.getTaskTracker(taskTrackerPkg.getTaskTrackerName()).removeTask(taskid);
        // 2. report to the job this task belongs to
        JobMeta job = this.jobTracker.getJob(task.getJobID());
        job.reportFinishedTask(taskid);
        // 3. check whether this job finishes
        if (job.isDone()) {
          job.setStatus(JobMeta.JobStatus.SUCCEED);
        }
        // 4. do task scheduling
        this.jobTracker.distributeTasks();
      } else if (taskProg.getStatus() == TaskMeta.TaskStatus.FAILED) {
        System.out.println("Task " + taskProg.getTaskID() + " failed.");
        task.increaseAttempts();
        if (task.getAttempts() <= TaskMeta.MAX_ATTEMPTS) {
          // retry this task
          this.jobTracker.submitTask(task);
        } else {
          // mark the job to which this task belongs as FAILED
          JobMeta job = this.jobTracker.getJob(task.getJobID());
          job.setStatus(JobMeta.JobStatus.FAILED);
        }
      }
    }
  }

  /**
   * the method to request next available job id
   */
  @Override
  public int requestJobID() throws RemoteException {
    return this.jobTracker.requestJobId();
  }

  /**
   * the method to submit a job
   */
  @Override
  public boolean submitJob(JobConf jconf) throws RemoteException {
    if (jconf == null)
      return false;

    int jid = jconf.getJobID();

    // prepare the code for each jobs
    if (!this.jobTracker.extractJobClassJar(jid, jconf.getJarFilePath())) {
      System.out.println("Extracting jar file error.");
      return false;
    }

    // update the job class information with the new place
    JobMeta newjob = new JobMeta(jconf);
    this.jobTracker.submitJob(newjob);

    // trigger the task scheduler
    this.jobTracker.distributeTasks();

    return true;
  }

  /**
   * the method for reducers to check the status of the mapper phase
   */
  @Override
  public MapStatusChecker.MapStatus checkMapStatus(int tid) throws RemoteException {
    MapStatusChecker.MapStatus result = this.jobTracker.checkMapStatus(tid);
    return result;
  }

}
