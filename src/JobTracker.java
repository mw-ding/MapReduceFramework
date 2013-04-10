import java.io.*;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class JobTracker {
	
	public final static String JOBTRACKER_SERVICE_NAME = "JobTrackerService";

	// all the tasktrackers running under current system
	private Map<String, TaskTrackerMeta> tasktrackers;
	
	// all the map tasks, including accomplished, executing, and uninitiated
	private Map<Integer, TaskMeta> mapTasks;
	
	// all the reduce tasks, including accomplished, executing, and uninitiated
	private Map<Integer, TaskMeta> reduceTasks;
	
	// all the jobs, including accomplished, executing, and uninitiated
	private Map<Integer, JobMeta> jobs;
	
	// the map task queue that all map tasks that have not been arranged to execute
	private Queue<TaskMeta> mapTasksQueue;
	
	// the reduce task queue that all reduce tasks that have not been arranged to execute
	private Queue<TaskMeta> reduceTasksQueue;
	
	// all the communication services that this jobtracker provides
	private JobTrackerServices services;
	
	// the scheduler, which generally decides assign which map/reduce task of
	// which job to which tasktracker.
	private TaskScheduler scheduler;
	
	// the maximum assigned job id currently
	private int currentMaxJobId;
	
	// the maximum assigned task id currently
	private int currentMaxTaskId;

	public final static int SCHEDULER_POOL_SIZE = 8;
	
	public final static int ALIVE_CHECK_CYCLE_SEC = 4;
	
	// the directory to which all the classes that user submits are extracted 
	public final static String JOB_CLASSPATH = "bin" + File.separator;
	
	// the prefix of the directories that stores the classes for different jobs
	public final static String JOB_CLASSPATH_PREFIX = "job";
	
	/*****************************************/
	
	public JobTracker(String rh, int rp) throws RemoteException {
		this.currentMaxJobId = 0;
		this.currentMaxTaskId = 0;
		
		this.tasktrackers = Collections.synchronizedMap(new HashMap<String, TaskTrackerMeta>());
		this.mapTasks = Collections.synchronizedMap(new HashMap<Integer, TaskMeta>());
		this.reduceTasks = Collections.synchronizedMap(new HashMap<Integer, TaskMeta>());
		this.jobs = Collections.synchronizedMap(new HashMap<Integer, JobMeta>());
		
		this.mapTasksQueue = (Queue<TaskMeta>) (new PriorityQueue<TaskMeta>(10, new Comparator<TaskMeta>() {

			@Override
			public int compare(TaskMeta o1, TaskMeta o2) {
				return o1.getTaskID() - o2.getTaskID();
			}
			
		}));
		
		this.reduceTasksQueue = (Queue<TaskMeta>) (new PriorityQueue<TaskMeta>(10, new Comparator<TaskMeta>() {

			@Override
			public int compare(TaskMeta o1, TaskMeta o2) {
				return o1.getTaskID() - o2.getTaskID();
			}
			
		}));
				
		this.scheduler = new DefaultTaskScheduler(this.tasktrackers, this.mapTasksQueue, this.reduceTasksQueue);
		
		this.services = new JobTrackerServices(this);
		Registry reg = LocateRegistry.getRegistry(rh, rp);
		reg.rebind(JOBTRACKER_SERVICE_NAME, this.services);
		
		ScheduledExecutorService serviceSche = Executors.newScheduledThreadPool(SCHEDULER_POOL_SIZE);
		
		// start the task tracker alive checking
		TaskTrackerAliveChecker alivechecker = new TaskTrackerAliveChecker(this);
		serviceSche.scheduleAtFixedRate(alivechecker, ALIVE_CHECK_CYCLE_SEC, ALIVE_CHECK_CYCLE_SEC, TimeUnit.SECONDS);
	}
	
	/**
	 * Change the task scheduler at runtime
	 * 
	 * @param s
	 * 		a new scheduler
	 */
	public void setScheduler(TaskScheduler s) {
		this.scheduler = s;
	}
	
	/**
	 * Register a new tasktracker in this jobtracker
	 * @param tt
	 * 		the metadata of this trasktracker
	 */
	public void registerTaskTracker(TaskTrackerMeta tt) {
		if (tt == null) return ;
		
		if (this.tasktrackers.containsKey(tt.getTaskTrackerName())) {
			throw new RuntimeException("The TaskTracker \"" + tt.getTaskTrackerName() + "\" already exist.");
		} else {
			this.tasktrackers.put(tt.getTaskTrackerName(), tt);
		}
	}
	
	/**
	 * get the whole list of task trackers
	 * @return
	 */
	public Map<String, TaskTrackerMeta> getTaskTrackers() {
		return Collections.unmodifiableMap(this.tasktrackers);
	}
	
	/**
	 * retrieve a specific task tracker
	 * @param id
	 * 		the id of the task tracker
	 * @return
	 * 		the task tracker if it exist; null if not
	 */
	public TaskTrackerMeta getTaskTracker(String id) {
		if (this.tasktrackers.containsKey(id)) {
			return this.tasktrackers.get(id);
		} else {
			return null;
		}
	}
	
	public void deleteTaskTracker(String ttname) {
		if (ttname == null) return ;
		
		if (this.tasktrackers.containsKey(ttname)) {
			this.tasktrackers.remove(ttname);
			
			// TODO : restart those tasks running on this tasktracker.
		}
	}
	
	/**
	 * get the map tasks list
	 * @return
	 */
	public Map<Integer, TaskMeta> getMapTasks(){
		return Collections.unmodifiableMap(this.mapTasks);
	}
	
	/**
	 * get the reduce tasks list
	 * @return
	 */
	public Map<Integer, TaskMeta> getReduceTasks() {
		return Collections.unmodifiableMap(this.reduceTasks);
	}
	
	/**
	 * get next available job id
	 * @return
	 */
	public int requestJobId() {
		int result = (++this.currentMaxJobId);
		return result;
	}
	
	/**
	 * get next available task id
	 * @return
	 */
	public int requestTaskId() {
		int result = (++this.currentMaxTaskId);
		return result;
	}
	
	/**
	 * trigger the task scheduling to fill all those idle slots
	 */
	public void distributeTasks() {
		Map<Integer, String> schestrategies = null;
		synchronized (this.tasktrackers) {
			schestrategies = this.scheduler.scheduleTask();
		}
		
		if (schestrategies == null) 
			return ;
		
		for (Entry<Integer, String> entry : schestrategies.entrySet()) {
			Integer taskid = entry.getKey();
			
			// find the task meta data
			TaskMeta task = null;
			
			if (this.mapTasks.containsKey(taskid)) {
				task = this.mapTasks.get(taskid);
			}
			
			if (this.reduceTasks.containsKey(taskid)) {
				task = this.reduceTasks.get(taskid);
			}
			
			if (task == null)
				continue;
			
			// find the specific task tracker
			TaskTrackerMeta targetTasktracker = this.tasktrackers.get(entry.getValue());
			
			try {
				// assign the task to the tasktracker
				boolean res = targetTasktracker.getTaskTrackerServices().runTask(task.getTaskInfo());
				
				if (res) {
					// if this task has been submitted to a tasktracker successfully
					task.getTaskProgress().setStatus(TaskStatus.INPROGRESS);
				} else {
					// if this task is failed to be submitted, place it back to the queue
					if (task.isMapper()) {
						this.mapTasksQueue.offer(task);
					} else {
						this.reduceTasksQueue.offer(task);
					}
				}
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * extract the jar file into the system's ClassPath folder
	 * 
	 * @param jobid
	 * @param jarpath
	 * @return
	 */
	public boolean extractJobClassJar(int jobid, String jarpath) {
		try {
			JarFile jar = new JarFile(jarpath);
			Enumeration enums = jar.entries();
			String destDirPath = JobTracker.JOB_CLASSPATH + JobTracker.JOB_CLASSPATH_PREFIX + jobid + File.separator;
			
			System.out.println("Extracting file to " + destDirPath);
			
			// copy each file in jar archive one by one
			while (enums.hasMoreElements()) {
				JarEntry file = (JarEntry) enums.nextElement();
				
				File outputfile = new File(destDirPath + file.getName());
				if (outputfile.isDirectory()) {
					outputfile.mkdirs();
					continue;
				}
				
				InputStream is = jar.getInputStream(file);
				FileOutputStream fos = null;
				try {
					fos = new FileOutputStream(outputfile);
				} catch (FileNotFoundException e) {
					outputfile.getParentFile().mkdirs();
					fos = new FileOutputStream(outputfile);
				}
				
				while (is.available() > 0) {
					fos.write(is.read());
				}
				
				fos.close();
				is.close();
			}
		} catch (IOException e) {
			// TODO : handle this exception if the jar file cannot be found
			return false;
		}
		
		return true;
	}
	
	/**
	 * submit a new job
	 * @param meta
	 */
	public void submitJob(JobMeta newjob) {
		this.jobs.put(newjob.getJobId(), newjob);
		
		// split the input
		newjob.splitInput();
		List<JobMeta.InputBlock> blocks = newjob.getInputBlocks();
		
		Map<Integer, TaskMeta> mapTasks = new HashMap<Integer, TaskMeta>();
		Map<Integer, TaskMeta> reduceTasks = new HashMap<Integer, TaskMeta>();
		
		// create new map tasks for this job
		for (JobMeta.InputBlock block : blocks) {
			int taskid = this.requestTaskId();
			TaskInfo minfo = new TaskInfo(taskid, block.getFilePath(), block.getOffset(), block.getLength(), newjob.getMapperClassName(),
			          "", newjob.getReducerNum(), TaskType.MAPPER);
			TaskMeta mtask = new TaskMeta(taskid, minfo, new TaskProgress(taskid));
			
			mapTasks.put(taskid, mtask);
			newjob.addMapperTask(taskid);
		}
		
		// create new reduce tasks for this job
		int reducerNum = newjob.getReducerNum();
		for (int i = 0; i < reducerNum; i++) {
			int taskid = this.requestTaskId();
			TaskInfo rinfo = new TaskInfo(taskid, "", 0, 0, newjob.getReducerClassName(), "", newjob.getReducerNum(), TaskType.REDUCER);
			TaskMeta rtask = new TaskMeta(taskid, rinfo, new TaskProgress(taskid));
			
			reduceTasks.put(taskid, rtask);
			newjob.addReducerTask(taskid);
		}
		
		// submit these tasks into the system
		this.mapTasks.putAll(mapTasks);
		this.reduceTasks.putAll(reduceTasks);
		this.mapTasksQueue.addAll(mapTasks.values());
		this.reduceTasksQueue.addAll(reduceTasks.values());
	}
	
	public static void main(String[] args) {
		try {
			JobTracker jb = new JobTracker("127.0.0.1", 12345);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
}
