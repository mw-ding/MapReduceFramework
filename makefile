RMI_CODE_BASE=-Djava.rmi.server.codebase=file:MapReduce.jar

compile:
	cd src; make
	
jar:
	jar cf MapReduce.jar -C bin/ .

rmi:
	rmiregistry $(PORT) &
	
jobtracker:
	#export MAPREDUCE_HOME=/Users/huanchen/Documents/workspace/MapReduceFramework
	java -cp MapReduce.jar $(RMI_CODE_BASE) mapreduce.JobTracker

tasktracker:
	#setenv MAPREDUCE_HOME /afs/andrew.cmu.edu/usr23/huanchez/ds/project3/MapReduceFramework
	java -cp MapReduce.jar $(RMI_CODE_BASE) mapreduce.TaskTracker $(SEQ)
	
client:
	#java -cp MapReduce.jar $(RMI_CODE_BASE) mapreduce.JobClient &
	java -cp MapReduce.jar $(RMI_CODE_BASE) -Djava.security.policy=server.policy mapreduce.JobClient
	 
degreecount:
	java -cp degreecount.jar:MapReduce.jar $(RMI_CODE_BASE) example.degreecount.Main input output degreecount.jar
clean:
	rm -rf bin/*
