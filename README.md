Experimenting with Akka Cluster + Akka Stream StreamRefs.

This example is based on the [Akka Cluster example project](https://doc.akka.io/docs/akka/current/typed/cluster.html#example-project)

## Overview
This project uses two actors to coordinate a dynamic stream processing cluster:

 - `DataIngress`: the actor that manages and provides data sources
 - `DataProcessor`: the actor that requests `SourceRef` from `DataIngress`, and runs the materialized remote
    source with a data-processing `Sink`.
    
Instead of creating these two types of actors directly, we use Akka Cluster Sharding to manage them as "entities".
A cluster singleton actor, `JobManager`, is responsible for spawning new entities upon new members joining
the cluster. For each new member, up to `max-nr-per-node` entities of a given type can be created, unless the number
of entities of that given type has already exceeded the `max-nr` defined in `streams.conf`.

## Known bugs
The internal state of the `JobManager` singleton actor is not properly persisted at the moment (unlike `ShardCoordinator`).
As a result, if the `JobManager` moves from one node to another, it will lose all its internal states, which may result 
in duplicated entity IDs, and thus violate the single-writer principle.

To solve this issue, we can consult with the implementation of `ShardCoordinator`. Or, instead of relying on the internal
state of `JobManager`, we can consider querying the `ShardCoordinator` for the current entity state. Therefore, we can
ensure data consistency.

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
