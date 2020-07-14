package sample.cluster.streams

import akka.actor.Address
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ClusterEvent
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.sharding.ShardRegion.ClusterShardingStats
import akka.cluster.sharding.typed.GetClusterShardingStats
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.typed.{Cluster, Subscribe}
import com.typesafe.config.ConfigFactory
import io.circe.config.syntax._

import scala.concurrent.duration._

/**
 * A cluster singleton actor that is responsible for creating the sharded entities (tasks.)
 */
object JobManager {
  sealed trait Command

  case class MemberEventResponse(memberEvent: MemberEvent) extends Command
  case class ClusterShardingStatsResponse(taskName: String, stats: ClusterShardingStats) extends Command
  case object ProcessMemberState extends Command

  def initialized(entityHelpers: Map[String, EntityTypeHelper[_]],
                  allocatedEntities: Map[String, List[EntityId]], taskResources: TaskResources,
                  clusterSharding: ClusterSharding,
                  stashedMemberEvents: List[MemberEvent],
                  upMembers: Int): Behavior[Command] = Behaviors.setup { ctx =>
    Behaviors.withTimers { timerScheduler =>
      Behaviors.receive {
        case (ctx, MemberEventResponse(evt)) =>
          if (evt.isInstanceOf[ClusterEvent.MemberUp] || evt.isInstanceOf[ClusterEvent.MemberExited]) {
            ctx.log.info(s"MemberEvent: ${evt.getClass.getSimpleName} - ${evt.member}")

            if (stashedMemberEvents.isEmpty) {
              // set a timer to process all member events in one go
              ctx.log.info("Set a timer to process member events in 10 seconds")
              timerScheduler.startSingleTimer(ProcessMemberState, 10.seconds)
            }
            initialized(entityHelpers, allocatedEntities, taskResources, clusterSharding, evt :: stashedMemberEvents, upMembers)
          } else {
            Behaviors.same
          }
        case (ctx, ProcessMemberState) =>
          val netChanges = stashedMemberEvents.collect {
            case ClusterEvent.MemberUp(_) => 1
            case ClusterEvent.MemberExited(_) => -1
          }.sum

          val updatedAllocated = entityHelpers
            .foldLeft(allocatedEntities) { case (allocated, (taskName, helper)) =>
              if (netChanges > 0) {
                val entityCount = allocated.get(taskName).map(_.length).getOrElse(0)
                val constraint = taskResources.taskParallelism.get(taskName)

                constraint.map(_.maxNr - entityCount).filter(_ > 0).map { gap =>
                  val entitiesToCreate = constraint.get.maxNrPerNode.map(_ * netChanges).getOrElse(Int.MaxValue) min gap
                  ctx.log.info(s"Need to create $entitiesToCreate entities for task: $taskName")

                  (1 to entitiesToCreate).foldLeft(allocated.getOrElse(taskName, Nil)) { case (entIds, index) =>
                    val entityId = EntityId(s"$taskName-${entityCount + index}")
                    ctx.log.info(s"Creating entity: $entityId")

                    // implicitly create a new entity through the EntityRef
                    helper.spawnEntity(clusterSharding, entityId)

                    entityId :: entIds
                  }
                } match {
                  case Some(entityIds) => allocated.updated(taskName, entityIds)
                  case None => allocated
                }
              } else if (netChanges < 0) {
                val entityNrToRemove = for {
                  entityCount <- allocated.get(taskName).map(_.length)
                  constraint <- taskResources.taskParallelism.get(taskName)
                  maxNrPerNode <- constraint.maxNrPerNode
                } yield {
                  maxNrPerNode * (upMembers + netChanges) - entityCount
                }

                entityNrToRemove match {
                  case Some(nr) if nr > 0 =>
                    val updatedEntityIds = (1 to nr).foldLeft(allocated(taskName)) { case (entityIds, _) =>
                      if (entityIds.nonEmpty) {
                        ctx.log.info(s"Killing Entity ${entityIds.head} for task $taskName")

                        helper.killEntity(clusterSharding, entityIds.head)
                        entityIds.tail
                      } else Nil
                    }

                    allocated.updated(taskName, updatedEntityIds)
                  case _ => allocated
                }
              } else allocated
            }

          initialized(entityHelpers, updatedAllocated, taskResources, clusterSharding, Nil, upMembers + netChanges)
        case _ =>
          Behaviors.same
      }
    }
  }

  def apply(entityHelpers: Map[String, EntityTypeHelper[_]]): Behavior[Command] = Behaviors.setup { ctx =>
    ctx.log.info("Initializing JobManager")

    // load configurations
    val taskResources = ConfigFactory
      .load("streams.conf")
      .as[TaskResources]("streams")
      .fold(ex => throw ex, identity)

    val cluster = Cluster(ctx.system)
    val clusterSharding = ClusterSharding(ctx.system)

    cluster.subscriptions ! Subscribe(
      ctx.messageAdapter[MemberEvent](MemberEventResponse),
      classOf[MemberEvent]
    )
    initialized(entityHelpers, Map.empty, taskResources, clusterSharding, Nil, 0)
  }
}
