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
      /* if there is free mapper slots */
      if (taskTracker.mapperCounter.incrementAndGet() <= taskTracker.NUM_OF_MAPPER_SLOTS) {
        /* TODO: start new process */
        String[] args = new String[] { MapperWorker.class.getName(),
            String.valueOf(taskInfo.getTaskID()), taskInfo.getInputPath(),
            String.valueOf(taskInfo.getOffset()), String.valueOf(taskInfo.getBlockSize()),
            taskInfo.getOutputPath(), taskInfo.getCodePath(),
            String.valueOf(taskInfo.getReducerNum()), taskTracker.getTaskTrackerName() };
        try {
          Utility.startJavaProcess(args);
        } catch (Exception e) {
          e.printStackTrace();
        }
        return true;
      } else {
        return false;
      }
    } else {
      /* if there is free reducer slots */
      if (taskTracker.reducerCounter.incrementAndGet() <= taskTracker.NUM_OF_REDUCER_SLOTS) {
        /* TODO: start new process */
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
    Map<Integer, TaskProgress> taskStatus = this.taskTracker.getTaskStatus();
    synchronized (taskStatus) {
      // taskProgress.setTimestamp(System.currentTimeMillis());
      taskStatus.put(taskProgress.getTaskID(), taskProgress);
    }
  }
}
