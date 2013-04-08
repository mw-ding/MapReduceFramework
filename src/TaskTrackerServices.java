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

  public boolean runTask(TaskInfo taskInfo) throws RemoteException {
    /* TODO: need to make it asynchronized */
    Worker worker;
    if (taskInfo.type == TaskType.MAPPER) {
      worker = new MapperWorker(taskInfo, taskTracker.getRegistryHostName(),
              taskTracker.getRegistryPort(), taskTracker.getTaskTrackerName());
      synchronized (taskTracker.mapperCounter) {
        if (taskTracker.mapperCounter.get() < taskTracker.NUM_OF_MAPPER_SLOTS) {
          /* TODO: start new process */
          return true;
        } else {
          return false;
        }
      }
    } else {
      worker = new ReducerWorker(taskInfo, taskTracker.getRegistryHostName(),
              taskTracker.getRegistryPort(), taskTracker.getTaskTrackerName());
      synchronized (taskTracker.reducerCounter) {
        if (taskTracker.reducerCounter.get() < taskTracker.NUM_OF_REDUCER_SLOTS) {
          /* TODO: start new process */
          return true;
        } else {
          return false;
        }
      }
    }
  }

  public void update(Object statuspck) throws RemoteException {
    TaskProgress taskProgress = (TaskProgress) statuspck;
    Map<String, TaskProgress> taskStatus = this.taskTracker.getTaskStatus();
    synchronized (taskStatus) {
      taskStatus.put(taskProgress.taskID, taskProgress);
    }
  }
}
