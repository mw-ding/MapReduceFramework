import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;

public class TaskTrackerServices extends UnicastRemoteObject implements TaskLauncher, StatusUpdater {

  private TaskTracker taskTracker;

  private static String RUN_MAPPER_CMD = "java " + MapperWorker.class.getName();

  private static String RUN_REDUCER_CMD = "java " + ReducerWorker.class.getName();

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
    /* if this is a mapper task */
    if (taskInfo.getType() == TaskType.MAPPER) {
      /* if there is free mapper slots */
      if (taskTracker.mapperCounter.incrementAndGet() <= taskTracker.NUM_OF_MAPPER_SLOTS) {
        /* TODO: start new process */
        ProcessBuilder pb = new ProcessBuilder(TaskTrackerServices.RUN_MAPPER_CMD,
                String.valueOf(taskInfo.getTaskID()), taskInfo.getInputPath(),
                String.valueOf(taskInfo.getOffset()), String.valueOf(taskInfo.getBlockSize()),
                taskInfo.getOutputPath(), taskInfo.getCodePath(), taskTracker.getTaskTrackerName());
        try {
          pb.start();
        } catch (IOException e) {
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
        ProcessBuilder pb = new ProcessBuilder(TaskTrackerServices.RUN_MAPPER_CMD,
                String.valueOf(taskInfo.getTaskID()), taskInfo.getInputPath(),
                String.valueOf(taskInfo.getOffset()), String.valueOf(taskInfo.getBlockSize()),
                taskInfo.getOutputPath(), taskInfo.getCodePath(), taskTracker.getTaskTrackerName());
        try {
          pb.start();
        } catch (IOException e) {
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
    Map<Integer, TaskProgress> taskStatus = this.taskTracker.getTaskStatus();
    synchronized (taskStatus) {
      // taskProgress.setTimestamp(System.currentTimeMillis());
      taskStatus.put(taskProgress.getTaskID(), taskProgress);
    }
  }
}
