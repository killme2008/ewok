# Introduction
A  high available BTM transaction logs journal using bookkeeper.
It's still under heavy development.

BTM: http://docs.codehaus.org/display/BTM/Home

BookKeeper:  http://zookeeper.apache.org/bookkeeper/svn.html

# License

BTM is LGPL license based,so is ewok.


#Internal

Ewok use zookeeper to store every TransactionManager's ledger id:

	 /ewok
		/app1
				/192-168-1-100 -- 0		[ledger id]
					 /ownership   				[Ephemeral node]
				/192-168-1-101 -- 1
				  	 /ownership
		/app2
				/192-168-1-135 -- 99
				/192-168-1-136 -- 100
					/ownership
				......

And ewok use bookkeeper to write transaction logs,it writes a TransactionLogRecord as an entry to ledger.If it write failed,it just create a new ledger and copy all dangling transactions to the new ledger.

#Configuration

Ewok use a config file named ewok-config.properties to configure it's properties:

	 #zookeepere servers
	 ewok.zkServers=10.232.102.191:2181
	 #special server id for this tm,default is local address
	 ewok.serverId=test
	 #zookeeper namespace
	 ewok.zkRoot=ewok
	 #zookeeper session timeout
	 ewok.zkSessionTimeout=5000
	 #A path to load other TM's logs,default is null
	 ewok.loadZkPath=
	 #Ledger ensemble size
	 ewok.ensembleSize=1
	 #Ledger quorum size
	 ewok.quorumSize=1
	 #Ledger password
	 ewok.password=ewok
	 #Batch size to read entries from ledger
	 ewok.cursorBatchSize=5
