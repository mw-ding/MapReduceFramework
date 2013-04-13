package mapreduce;
import java.util.List;


public abstract class Reducer {
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
	
	public abstract void reduce(String key, List<String> values, Outputer out);
}
