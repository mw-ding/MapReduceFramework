package mapreduce;
import java.io.Serializable;
import java.util.List;

/**
 * This class is used to update task tracker status to job tracker.
 *
 */
public class TaskTrackerUpdatePkg implements Serializable {

  /* the name of the task tracker */
	private String taskTrackerName;

	/* the number of maper slots still available */
	private int numOfMapperSlots;

	/* the number of reducer slots still available */
	private int numOfReducerSlots;

	/* the status of tasks running on task tracker */
	private List<TaskProgress> taskStatus;
	
	/* the service name of task tracker: where to contact of necessary */
	private String serviceName;
	
	/* the task tracker registry host name */
	private String hostname;
	
	/* the task tracker registry port */
	private int rPort;

	/**
	 * constructor method 
	 * @param taskTrackerName
	 * @param numOfMapperSlots
	 * @param numOfReducerSlots
	 * @param sn
	 * @param taskStatus
	 */
	public TaskTrackerUpdatePkg(String taskTrackerName, int numOfMapperSlots,
			int numOfReducerSlots, String sn, List<TaskProgress> taskStatus, int port) {
		this.taskTrackerName = taskTrackerName;
		this.numOfMapperSlots = numOfMapperSlots;
		this.numOfReducerSlots = numOfReducerSlots;
		this.taskStatus = taskStatus;
		this.serviceName = sn;
		this.rPort = port;
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

	public String getTaskTrackerName() {
		return taskTrackerName;
	}

	public String getServiceName() {
		return serviceName;
	}
	
	public int getRPort(){
	  return this.rPort;
	}
}
