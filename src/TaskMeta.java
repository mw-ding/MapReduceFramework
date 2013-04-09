public class TaskMeta {
  private int taskID;

  private TaskInfo taskInfo;

  private TaskProgress taskProgress;

  public TaskMeta(int TaskID, TaskInfo taskInfo, TaskProgress taskProgress) {
    this.taskInfo = taskInfo;
    this.taskProgress = taskProgress;
  }

  public TaskMeta(int taskID, TaskInfo taskInfo) {
    this.taskID = taskID;
    this.taskInfo = taskInfo;
  }

  public int getTaskID() {
    return this.taskID;
  }

  public TaskInfo getTaskInfo() {
    return taskInfo;
  }

  public void setTaskInfo(TaskInfo taskInfo) {
    this.taskInfo = taskInfo;
  }

  public TaskProgress getTaskProgress() {
    return taskProgress;
  }

  public void setTaskProgress(TaskProgress taskProgress) {
    this.taskProgress = taskProgress;
  }

  public void setTaskID(int taskID) {
    this.taskID = taskID;
  }

}
