RMI_CODE_BASE=-Djava.rmi.server.codebase=file:$(MAPREDUCE_HOME)/bin/
CLASSPATH=$(MAPREDUCE_HOME)/bin/
compile:
	cd src; make
	
jar:
	jar cf MapReduce.jar -C bin/ .

rmi:
	cd $(CLASSPATH); rmiregistry $(PORT) &
	
jobtracker:
	java -cp $(CLASSPATH) $(RMI_CODE_BASE) mapreduce.JobTracker

tasktracker:
	java -cp $(CLASSPATH) $(RMI_CODE_BASE) mapreduce.TaskTracker $(SEQ)
	
client:
	java -cp $(CLASSPATH) $(RMI_CODE_BASE) mapreduce.JobClient &
	 
degreecount:
	java -cp degreecount.jar:$(CLASSPATH) $(RMI_CODE_BASE) example.degreecount.Main input $(OUTPUT) degreecount.jar
wordcount:
	java -cp wordcount.jar:$(CLASSPATH) $(RMI_CODE_BASE) example.wordcount.Main input $(OUTPUT) wordcount.jar
clean:
	rm -rf bin/* MapReduce.jar
