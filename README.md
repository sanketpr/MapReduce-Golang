### MapReduce Implementation
- This is a simple implementation of [MapReduce Paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) in Golang
- The implementaion is based on assumption that we have a shared memory
- All the communications are done using `RPC` library over `TCP` connection

#### Approach:
- There are two types of nodes for our job, a `Master` node and multiple `Workers` node
- `Master` is responsible for dividing, assigning and coordinating the job across all the workers and producing final results
- Typically a `Worker` node is responsible for performing the real job on a subset of data
- Depending on the type of worker, it will eigher be working on `Mapper` task or `Reducer` task
 
**Master Node [(/mr/master.go)](https://github.com/sanketpr/MapReduce-Golang-InProgress-/blob/master/src/mr/master.go) :**
- The program starts with starting master node. On starting the node we create an instance of master, Setup input data, Start the RPC server, regestering the struct with for RPC and listen to incoming connection request from the Wroker server
- When the Master node is done setting up it signlas a Worker node to spawn Mappers and Reducers 
- The Master node provides Worker node with the necessacery information like number of mappers (based on the partitions of data) and number of reducers 
- Mapper Phase, after all the Mappers are registered with Master, all the workers are assigned the map job along with chuck of input data and wait for them to complete their job
- On completion of the mapper phase master collects data from all the mappers and proceeds with Collection and Sorting phase
where the data is combined and sorted by the Key values(A Mapper emits its output in form of key and value)
- And finally the this data is partitioned into number of set depending on number of Reducers available(or assigned). And using this data the final phase, Reduce phase is scheduled across N reducers
- After completion the data is combined and written to the output file


**Worker Node [(/mr/worker.go)](https://github.com/sanketpr/MapReduce-Golang-InProgress-/blob/master/src/mr/worker.go) :**
- On the worker side, we start by establising connection with the Master node
- The master tells how many Mapper and Reducer nodes are required. And accordingly respective worker nodes are spawned aschronconusly
- The worker nodes generate a unique address for themselfves and start a RPC server
- Each worker node after setup registers itself with Master using its RPC server address so master communicate and provide job details

#### Result: 
Output: [main/mr-out-0](https://github.com/sanketpr/MapReduce-Golang-InProgress-/blob/master/src/main/mr-out-0)

![Sample Output](https://github.com/sanketpr/MapReduce-Golang-InProgress-/blob/master/src/main/Screenshots/Sample_Output.png)


#### Screenshots:
![Master Img1](https://github.com/sanketpr/MapReduce-Golang-InProgress-/blob/master/src/main/Screenshots/Master-IMG1.png)
![Master Img2](https://github.com/sanketpr/MapReduce-Golang-InProgress-/blob/master/src/main/Screenshots/Master-IMG2.png)
![Worker Img1](https://github.com/sanketpr/MapReduce-Golang-InProgress-/blob/master/src/main/Screenshots/Worker-IMG1.png)







Base code from: (https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
