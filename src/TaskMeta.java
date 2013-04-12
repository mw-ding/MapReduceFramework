public class TaskMeta {
  /* the id of the job to which this task belongs */
  private int jobID;

  /* the id of the task */
  private int taskID;

  /* task information like input path, output path, code path etc. */
  private TaskInfo taskInfo;

  /* the progress and status of one task */
  private TaskProgress taskProgress;

  public TaskMeta(int TaskID, int JobID, TaskInfo taskInfo, TaskProgress taskProgress) {
    this.jobID = JobID;
    this.taskID = TaskID;
    this.taskInfo = taskInfo;
    this.taskProgress = taskProgress;
  }

  public TaskMeta(int taskID, int JobID, TaskInfo taskInfo) {
    this.jobID = JobID;
    this.taskID = taskID;
    this.taskInfo = taskInfo;
  }

  public int getJobID() {
    return this.jobID;
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

  public TaskType getType() {
    return this.taskInfo.getType();
  }

  public boolean isMapper() {
    return this.getType() == TaskType.MAPPER;
  }

  public boolean isReducer() {
    return this.getType() == TaskType.REDUCER;
  }
  
  public boolean isDone() {
    return this.getTaskProgress().getStatus() == TaskStatus.SUCCEED;
  }
}
