import java.io.Serializable;
import java.util.List;

public class TaskTrackerUpdatePkg implements Serializable {
  int numOfMapperSlots;

  int numOfReducerSlots;

  List<TaskProgress> taskStatus;

  public TaskTrackerUpdatePkg(int numOfMapperSlots, int numOfReducerSlots,
          List<TaskProgress> taskStatus) {
    this.numOfMapperSlots = numOfMapperSlots;
    this.numOfReducerSlots = numOfReducerSlots;
    this.taskStatus = taskStatus;
  }
}
