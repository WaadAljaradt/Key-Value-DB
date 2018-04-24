# Key-Value-DB

                Mini Key-Value data base CP system.
# General Intorduction 

Three nodes serving as : Master, Node2 , Node3 (AWS EC2) in the system each one in
configured to be in three different subnets in a VPC network In the test cases using Postman
application to hit Post requests to the load balancer (with the 3 instances healthy )with the four
operations allowed in the system : 
Store(key,value) , Fetch(key) Delete(key), Update(Key,value) 
Healthy functioning system that stores, fetch , update and delete across the three nodes
with data consistent among all three nodes and return Json response back to User.

# Healthy status 
In a Healthy System, a Master can execute Read/Write operations and then
propagate the changes through internal rest Api’s (duplicate changes) to other nodes to assure
consistency among other nodes, in case of failure it keeps a log file of failed operations, values
for each node in the system. Slave Nodes in the other hand , check if they are not in Partition
Mode ( that means they are able to communicate with master) they can execute read form their data, and they forward write Operations to Master to
handle write and wait for response of successful forwarding Status and replications (through
fetch and compare to requested values) in Case of Successful it return to User with expected
results ( consistent ).

# Partition 
Partition implemented by closing ports of communications in security-groups of instances.
Jump box instance in the same network is used to test partition state.
Partition : Slave Nodes are Irresponsive ( Not available) , Master keep track for all the failed operations for
each node in partition in a file and propagate to in-service ones and then handle requests and
return response to Users.

# Partition Recovery
When a node comes back to the system, it communicate with the master
through a group of logical apis to achieve consistency before if can serve Users, it start with a
“am I synch ?” type of question in a /IsSynch rest Api to master if the answer is No it will
request all the changes that it has missed from the Master that has a log of all of these and
should make sure to propagate them. then Node would be ready to serve the request to User.

# Configurable setting consistent to a available DB mode

Slave checks if Im in Partition Mode and Conf saved value after Conf Api is set to value =2 (AP)
configured, or v=1 is CP then readings of latest data in partition mode for AP system are
returned to user and out-service if CP Configured


# Youtube Demo links :

1- Consistent system 
https://www.youtube.com/watch?v=mETNDgfdQGQ&t=9s

2- Configrable system (Consistent or Available) 

https://www.youtube.com/watch?v=b5Izrlsh_Ug&t=567s
