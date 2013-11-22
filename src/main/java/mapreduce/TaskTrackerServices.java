package mapreduce;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;

/**
 * This class is the rmi services provided by task tracker
 */
public class TaskTrackerServices extends UnicastRemoteObject implements TaskLauncher, StatusUpdater {

  /* reference to the task tracker */
  private TaskTracker taskTracker;

  /**
   * constructor method
   * 
   * @param taskTracker
   * @throws RemoteException
   */
  public TaskTrackerServices(TaskTracker taskTracker) throws RemoteException {
    super();
    this.taskTracker = taskTracker;
  }

  /**
   * This method is called by job tracker to assign task to task tracker
   * 
   * @param taskInfo
   *          : the information about the task
   * @return the task is submitted successfully or not
   */
  public boolean runTask(TaskInfo taskInfo) throws RemoteException {
    /* if this is a mapper task */
    if (taskInfo.getType() == TaskMeta.TaskType.MAPPER) {
      MapperTaskInfo mapperTaskInfo = (MapperTaskInfo) taskInfo;
      /* lock the mapper counter */
      synchronized (taskTracker.mapperCounter) {

        /* if there is free mapper slots */
        if (taskTracker.mapperCounter < taskTracker.NUM_OF_MAPPER_SLOTS) {

          /* do some logging */
          System.out.println("task tracker " + this.taskTracker.getTaskTrackerName()
                  + " received runTask request taskid:" + taskInfo.getTaskID() + " "
                  + taskInfo.getType());

          /* increase number of mapper running on this task tracker */
          taskTracker.mapperCounter++;

          /* start new process */
          String[] args = new String[] { MapperWorker.class.getName(),
              String.valueOf(mapperTaskInfo.getTaskID()), mapperTaskInfo.getInputPath(),
              String.valueOf(mapperTaskInfo.getOffset()),
              String.valueOf(mapperTaskInfo.getBlockSize()), mapperTaskInfo.getOutputPath(),
              mapperTaskInfo.getMapper(), mapperTaskInfo.getPartitioner(),
              mapperTaskInfo.getInputFormat(), String.valueOf(mapperTaskInfo.getReducerNum()),
              taskTracker.getTaskTrackerName(), String.valueOf(taskTracker.getRPort()) };
          try {
            Utility.startJavaProcess(args, taskInfo.getJobID());
          } catch (Exception e) {
            e.printStackTrace();
          }
          return true;
        } else {
          return false;
        }
      }
    } else {
      /* this is a reducer task */
      ReducerTaskInfo reducerTaskInfo = (ReducerTaskInfo) taskInfo;

      /* lock the reducer counter */
      synchronized (taskTracker.reducerCounter) {
        /* if there is free reducer slots */
        if (taskTracker.reducerCounter < taskTracker.NUM_OF_REDUCER_SLOTS) {

          /* do some logging */
          System.out.println("task tracker " + this.taskTracker.getTaskTrackerName()
                  + " received runTask request taskid:" + taskInfo.getTaskID() + " "
                  + taskInfo.getType());

          /* increase the number of reducer running on this task tracker */
          taskTracker.reducerCounter++;

          /* start new process */
          String[] args = new String[] { ReducerWorker.class.getName(),
              String.valueOf(reducerTaskInfo.getTaskID()),
              String.valueOf(reducerTaskInfo.getOrderId()), reducerTaskInfo.getReducer(),
              reducerTaskInfo.getOutputFormat(), reducerTaskInfo.getInputPath(),
              reducerTaskInfo.getOutputPath(), taskTracker.getTaskTrackerName(),
              String.valueOf(taskTracker.getRPort()) };
          try {
            Utility.startJavaProcess(args, taskInfo.getJobID());
          } catch (Exception e) {
            e.printStackTrace();
          }
          return true;
        } else {
          return false;
        }
      }
    }
  }

  /**
   * this method is called by task worker to update task status to task tracker
   */
  public void update(Object statuspck) throws RemoteException {
    if (statuspck.getClass().getName() != TaskProgress.class.getName())
      return;
    TaskProgress taskProgress = (TaskProgress) statuspck;
    /* do some logging */
    System.out.println(System.currentTimeMillis() + " Receive update from worker "
            + taskProgress.getTaskID() + " : " + taskProgress.getStatus());

    /* update the status to task tracker */
    Map<Integer, TaskProgress> taskStatus = this.taskTracker.getTaskStatus();
    synchronized (taskStatus) {
      taskStatus.put(taskProgress.getTaskID(), taskProgress);
    }
  }
}
