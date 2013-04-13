package mapreduce;

public abstract class Mapper {
	
	/**
	 * the method to do some initialization work. The method
	 * would only be called once at the very beginning of this
	 * task
	 */
	protected void setup() {
		
	}
	
	/**
	 * the method to do some cleaning job. The method would only
	 * be called once at the very end of this task
	 */
	protected void cleanup() {
	  
	}
	
	public abstract void map(String key, String value, Outputer out);
}
