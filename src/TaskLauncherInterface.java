import java.rmi.Remote;

public interface TaskLauncherInterface extends Remote {
  TaskOutput runTask(TaskInfo taskinfo);
}
