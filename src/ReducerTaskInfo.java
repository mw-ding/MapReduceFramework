import java.io.File;

public class ReducerTaskInfo extends TaskInfo {

  private int taskId;
  
  /* the order of this reduce task */
  private int orderId;

  /* the input file path */
  private String inputPath;

  /* the class name of reducer */
  private String reducer;

  /* the output format */
  private String outputFormat;

  /* the output path */
  private String outputPath;

  public ReducerTaskInfo(int taskID, int order, String jobMapperOutputDir, String r, String of, String op) {
    super(taskID, TaskType.REDUCER);
    this.taskId = taskID;
    this.orderId = order;
    this.inputPath = jobMapperOutputDir;
    this.reducer = r;
    this.outputFormat = of;
    this.outputPath = op;
  }

  public int getTaskId() {
    return this.taskId;
  }
  
  public int getOrderId() {
    return this.orderId;
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
