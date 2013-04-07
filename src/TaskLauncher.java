import java.rmi.Remote;

public interface TaskLauncher extends Remote {
  TaskOutput runTask(TaskInfo taskinfo);
}
