RMI_CODE_BASE=-Djava.rmi.server.codebase=file:MapReduce.jar
compile:
	cd src; make
	
jar:
	jar cf MapReduce.jar -C bin/ .

rmi:
	rmiregistry $(PORT)
	
jobtracker:
	export MAPREDUCE_HOME=/Users/huanchen/Documents/workspace/MapReduceFramework
	java -cp MapReduce.jar $(RMI_CODE_BASE) mapreduce.JobTracker

dispatcher:
	java -cp ./bin Dispatcher $(DPORT) $(RIP) $(RPORT)

testclient:
	java -cp ./bin TestClient $(RIP) $(RPORT)

clean:
	rm -rf bin/*
