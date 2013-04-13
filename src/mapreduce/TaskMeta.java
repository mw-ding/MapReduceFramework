package mapreduce;
public class TaskMeta {
  
  public enum TaskStatus {
    INIT, INPROGRESS, FAILED, SUCCEED
  }

  public enum TaskType {
    MAPPER, REDUCER
  }

  // the maximum number of attempts, after which this task is
  // sentenced to death
  public final static int MAX_ATTEMPTS = 1;
  
  /* the id of the job to which this task belongs */
  private int jobID;

  /* the id of the task */
  private int taskID;

  /* task information like input path, output path, code path etc. */
  private TaskInfo taskInfo;

  /* the progress and status of one task */
  private TaskProgress taskProgress;
  
  // the number of tries to execute this task
  private int attempts;  

  public TaskMeta(int TaskID, int JobID, TaskInfo taskInfo, TaskProgress taskProgress) {
    this.jobID = JobID;
    this.taskID = TaskID;
    this.taskInfo = taskInfo;
    this.taskProgress = taskProgress;
    this.attempts = 1;
  }

  public TaskMeta(int taskID, int JobID, TaskInfo taskInfo) {
    this.jobID = JobID;
    this.taskID = taskID;
    this.taskInfo = taskInfo;
    this.attempts = 1;
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
  
  public void increaseAttempts() {
    this.attempts ++;
  }
  
  public int getAttempts() {
    return this.attempts;
  }
}
