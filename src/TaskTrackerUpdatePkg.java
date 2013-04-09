import java.io.Serializable;
import java.util.List;

public class TaskTrackerUpdatePkg implements Serializable {

  private int numOfMapperSlots;

  private int numOfReducerSlots;

  private List<TaskProgress> taskStatus;

  public TaskTrackerUpdatePkg(int numOfMapperSlots, int numOfReducerSlots,
          List<TaskProgress> taskStatus) {
    this.numOfMapperSlots = numOfMapperSlots;
    this.numOfReducerSlots = numOfReducerSlots;
    this.taskStatus = taskStatus;
  }

  public int getNumOfMapperSlots() {
    return numOfMapperSlots;
  }

  public int getNumOfReducerSlots() {
    return numOfReducerSlots;
  }

  public List<TaskProgress> getTaskStatus() {
    return taskStatus;
  }
}
