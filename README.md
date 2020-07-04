Experimenting with Akka Cluster + Akka Stream StreamRefs.

This example is based on the [Akka Cluster example project](https://doc.akka.io/docs/akka/current/typed/cluster.html#example-project)

## Overview
This project uses two actors to coordinate a dynamic stream processing cluster:

 - `DataIngressProcessor`: the actor that manages and provides data sources
 - `DataProcessor`: the actor that requests `SourceRef` from `DataIngressProcessor`, and runs the materialized remote
    source with a data-processing `Sink`.
    
These two types of worker actors are spawned by the `TaskSlotManager` actor, which has a fixed amount of "task slots"
which can be filled with either `DataIngressProcessor` or `DataProcessor`.

The task slot assignment is coordinated by the cluster singleton actor, `TaskCoordinator`, which manages the overall
number of tasks, and assigns roles to `TaskSlotManager`.

## Usage 

### Same JVM
Running the application `sample.cluster.streams.App` starts multiple actor systems (cluster members) in the same JVM 
process. Each actor system spawns a `TaskSlotMananger`, which runs the worker actors accordingly.

### Multiple JVM
In the first terminal window, start the first seed node with the following command:

```
    sbt "runMain sample.cluster.streams.App 25251"
```

25251 corresponds to the port of the first seed-nodes element in the configuration. 

In the second terminal window, start the second seed node with the following command:

```
    sbt "runMain sample.cluster.streams.App 25252"
```

25252 corresponds to the port of the second seed-nodes element in the configuration. 

Start more nodes in additional terminal windows with arbitrary port number.
