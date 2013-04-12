import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;

public class TaskTrackerServices extends UnicastRemoteObject implements TaskLauncher, StatusUpdater {

  private TaskTracker taskTracker;

  public TaskTrackerServices(TaskTracker taskTracker) throws RemoteException {
    super();
    this.taskTracker = taskTracker;
  }

  /**
   * @param taskInfo
   *          : the information about the task
   * @return the task is submitted successfully or not
   */
  public boolean runTask(TaskInfo taskInfo) throws RemoteException {
    System.out.println("task tracker " + this.taskTracker.getTaskTrackerName()
            + " received runTask request taskid:" + taskInfo.getTaskID());
    System.out.println("task type " + taskInfo.getType());
    /* TODO: need to make it asynchronized */
    /* if this is a mapper task */
    if (taskInfo.getType() == TaskType.MAPPER) {
      MapperTaskInfo mapperTaskInfo = (MapperTaskInfo) taskInfo;
      /* if there is free mapper slots */
      if (taskTracker.mapperCounter.incrementAndGet() <= taskTracker.NUM_OF_MAPPER_SLOTS) {
        /* TODO: start new process */
        String[] args = new String[] { MapperWorker.class.getName(),
            String.valueOf(mapperTaskInfo.getTaskID()), mapperTaskInfo.getInputPath(),
            String.valueOf(mapperTaskInfo.getOffset()),
            String.valueOf(mapperTaskInfo.getBlockSize()), mapperTaskInfo.getOutputPath(),
            mapperTaskInfo.getMapper(), mapperTaskInfo.getPartitioner(),
            mapperTaskInfo.getInputFormat(), String.valueOf(mapperTaskInfo.getReducerNum()),
            taskTracker.getTaskTrackerName() };
        try {
          Utility.startJavaProcess(args, taskInfo.getJobID());
        } catch (Exception e) {
          e.printStackTrace();
        }
        return true;
      } else {
        return false;
      }
    } else {
      ReducerTaskInfo reducerTaskInfo = (ReducerTaskInfo) taskInfo;
      /* if there is free reducer slots */
      if (taskTracker.reducerCounter.incrementAndGet() <= taskTracker.NUM_OF_REDUCER_SLOTS) {
        /* TODO: start new process */
        String[] args = new String[] { ReducerWorker.class.getName(),
            String.valueOf(reducerTaskInfo.getTaskID()),
            String.valueOf(reducerTaskInfo.getOrderId()), reducerTaskInfo.getReducer(),
            reducerTaskInfo.getOutputFormat(), reducerTaskInfo.getInputPath(),
            reducerTaskInfo.getOutputPath(), taskTracker.getTaskTrackerName() };
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

  public void update(Object statuspck) throws RemoteException {
    if (statuspck.getClass().getName() != TaskProgress.class.getName())
      return;
    TaskProgress taskProgress = (TaskProgress) statuspck;
    System.out.println("Receive update from " + taskProgress.getType() + " worker : " + taskProgress.getStatus());
    Map<Integer, TaskProgress> taskStatus = this.taskTracker.getTaskStatus();
    synchronized (taskStatus) {
      // taskProgress.setTimestamp(System.currentTimeMillis());
      taskStatus.put(taskProgress.getTaskID(), taskProgress);
    }
  }
}
