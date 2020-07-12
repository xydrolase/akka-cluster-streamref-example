package sample.cluster.streams

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ClusterEvent
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.typed.{Cluster, Subscribe}
import com.typesafe.config.ConfigFactory
import io.circe.config.syntax._

/**
 * A cluster singleton actor that is responsible for creating the sharded entities (tasks.)
 */
object JobManager {
  sealed trait Command

  case class MemberEventResponse(memberEvent: MemberEvent) extends Command

  def initialized(entityCreators: Map[String, (ClusterSharding, EntityId) => Unit],
                  allocatedEntities: Map[String, List[EntityId]],
                  taskResources: TaskResources, clusterSharding: ClusterSharding): Behavior[Command] = {
    Behaviors.receive {
      case (ctx, MemberEventResponse(ClusterEvent.MemberUp(member))) =>
        ctx.log.info(s"Member is up: ${member.address}, roles: ${member.roles}")

        val updatedAllocated = entityCreators
          .foldLeft(allocatedEntities) { case (allocated, (taskName, creatorFn)) =>
            val entityCount = allocated.get(taskName).map(_.length).getOrElse(0)
            val constraint = taskResources.taskParallelism.get(taskName)

            constraint.map(_.maxNr - entityCount).filter(_ > 0).map { gap =>
              val entitiesToCreate = constraint.get.maxNrPerNode.getOrElse(Int.MaxValue) min gap
              ctx.log.info(s"Need to create $entitiesToCreate entities for task: $taskName")

              (1 to entitiesToCreate).foldLeft(allocated.getOrElse(taskName, Nil)) { case (entIds, index) =>
                val entityId = EntityId(s"$taskName-${entityCount + index}")
                ctx.log.info(s"Creating entity: $entityId")

                // implicitly create a new entity through the EntityRef;
                creatorFn(clusterSharding, entityId)

                entityId :: entIds
              }
            } match {
              case Some(entityIds) => allocated.updated(taskName, entityIds)
              case None => allocated
            }
          }

        initialized(entityCreators, updatedAllocated, taskResources, clusterSharding)
      case (ctx, MemberEventResponse(ClusterEvent.MemberExited(member))) =>
        Behaviors.same
      case _ =>
        Behaviors.same
    }
  }

  def apply(entityCreators: Map[String, (ClusterSharding, EntityId) => Unit]): Behavior[Command] = Behaviors.setup { ctx =>
    ctx.log.info("Initializing JobManager")

    // load configurations
    val taskResources = ConfigFactory
      .load("streams.conf")
      .as[TaskResources]("streams")
      .fold(ex => throw ex, identity)

    // subscribe to membership events
    val cluster = Cluster(ctx.system)
    cluster.subscriptions ! Subscribe(
      ctx.messageAdapter[MemberEvent](MemberEventResponse),
      classOf[MemberEvent]
    )

    val clusterSharding = ClusterSharding(ctx.system)

    initialized(entityCreators, Map.empty, taskResources, clusterSharding)
  }
}
