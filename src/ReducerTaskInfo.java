public class ReducerTaskInfo extends TaskInfo {
  
  private int taskId;
  
  /* the input file path */
  private String inputPath;

  /* the class name of reducer */
  private String reducer;

  /* the output format */
  private String outputFormat;

  /* the output path */
  private String outputPath;

  public ReducerTaskInfo(int taskID, String ip, String r,
          String of, String op) {
    super(taskID, TaskType.REDUCER);
    this.taskId = taskID;
    this.inputPath = ip;
    this.reducer = r;
    this.outputFormat = of;
    this.outputPath = op;
  }
  
  public int getTaskId() {
    return this.taskId;
  }

  public String getInputPath() {
    return inputPath;
  }

  public String getReducer() {
    return reducer;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public String getOutputPath() {
    return outputPath;
  }
}
