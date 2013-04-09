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
    /* TODO: need to make it asynchronized */
    Worker worker;
    /* if this is a mapper task */
    if (taskInfo.getType() == TaskType.MAPPER) {
      /* instantiate a mapper task */
      worker = new MapperWorker(taskInfo, taskTracker.getRegistryHostName(),
              taskTracker.getRegistryPort(), taskTracker.getTaskTrackerName());
      /* if there is free mapper slots */
      if (taskTracker.mapperCounter.incrementAndGet() <= taskTracker.NUM_OF_MAPPER_SLOTS) {
        /* TODO: start new process */

        return true;
      } else {
        return false;
      }
    } else {
      /* instantiate a reducer task */
      worker = new ReducerWorker(taskInfo, taskTracker.getRegistryHostName(),
              taskTracker.getRegistryPort(), taskTracker.getTaskTrackerName());
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
    if(statuspck.getClass().getName() != TaskProgress.class.getName())
      return;
    TaskProgress taskProgress = (TaskProgress) statuspck;
    Map<Integer, TaskProgress> taskStatus = this.taskTracker.getTaskStatus();
    synchronized (taskStatus) {
      taskProgress.setTimestamp(System.currentTimeMillis());
      taskStatus.put(taskProgress.getTaskID(), taskProgress);
    }
  }
}
