import java.io.Serializable;

public class JobConf implements Serializable {

	// the name for current job
	private String jobName;

	// the input path
	private String inputPath;

	// the output path
	private String outputPath;

	// the mapper class
	private Class<?> mapperClass;

	// the reducer class
	private Class<?> reducerClass;
	
	// TODO : now we assume that key and value are both String type
	// the key class
//	private Class<?> keyClass;
	
	// the value class
//	private Class<?> valueClass;

	public JobConf() {
		this.jobName = "";

		this.inputPath = null;
		this.outputPath = null;
		this.mapperClass = null;
		this.reducerClass = null;
//		this.keyClass<?> = String.class;
//		this.valueClass<?> = String.class;
	}
	
	/**
	 * whether current josb configuration is valid for a job
	 * @return
	 */
	public boolean isValid() {
		// if any of the following field is null, then invalid
		if (this.inputPath == null)
			return false;
		
		if (this.outputPath == null)
			return false;
		
		if (this.mapperClass == null || !this.mapperClass.getName().equals(Mapper.class.getName()))
			return false;
		
		if (this.reducerClass == null || !this.reducerClass.getName().equals(Reducer.class.getName()))
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

	public Class<?> getMapperClass() {
		return mapperClass;
	}

	public void setMapperClass(Class<?> mapperClass) {
		this.mapperClass = mapperClass;
	}

	public Class<?> getReducerClass() {
		return reducerClass;
	}

	public void setReducerClass(Class<?> reducerClass) {
		this.reducerClass = reducerClass;
	}
}
