package mapreduce;
import java.io.File;

public class ReducerTaskInfo extends TaskInfo {

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

  public ReducerTaskInfo(int jid, int taskID, int order, String jobMapperOutputDir, String r, String of, String op) {
    super(jid, taskID, TaskMeta.TaskType.REDUCER);
    this.orderId = order;
    this.inputPath = jobMapperOutputDir;
    this.reducer = r;
    this.outputFormat = of;
    this.outputPath = op;
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
