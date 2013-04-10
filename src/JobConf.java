import java.io.Serializable;

public class JobConf implements Serializable {
  /* TODO: make set job id invisible to user */

	// the id for current job
	private int jobID;

	// the name for current job
	private String jobName;

	// the input path
	private String inputPath;

	// the output path
	private String outputPath;

	// the block size
	private int blockSize;
	
	// the number of reducers
	private int reducerNum;

	// the mapper class name
	private String mapperClassName;

	// the reducer class name
	private String reducerClassName;

	// TODO : now we assume that key and value are both String type
	// the key class
	// private Class<?> keyClass;

	// the value class
	// private Class<?> valueClass;

	public JobConf() {
		this.jobName = "";
		this.inputPath = null;
		this.outputPath = null;
		this.mapperClassName = null;
		this.reducerClassName = null;
		// this.keyClass<?> = String.class;
		// this.valueClass<?> = String.class;
	}

	/**
	 * whether current josb configuration is valid for a job
	 * 
	 * @return
	 */
	public boolean isValid() {
		// if any of the following field is null, then invalid
		if (this.inputPath == null)
			return false;

		if (this.outputPath == null)
			return false;

		if (this.mapperClassName == null)
			return false;

		if (this.reducerClassName == null)
			return false;

		if (this.blockSize == 0)
			return false;
		
		if(this.reducerNum == 0)
		  return false;

		return true;
	}

	// the getters and setters for all members
	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public int getJobID() {
		return jobID;
	}

	public void setJobID(int jobID) {
		this.jobID = jobID;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	public String getMapperClassName() {
		return mapperClassName;
	}

	public void setMapperClassName(String mapperClassName) {
		this.mapperClassName = mapperClassName;
	}

	public String getReducerClassName() {
		return reducerClassName;
	}

	public void setReducerClassName(String reducerClassName) {
		this.reducerClassName = reducerClassName;
	}
	
	public int getReducerNum() {
		return reducerNum;
	}

	public void setReducerNum(int reducerNum) {
		this.reducerNum = reducerNum;
	}
}
