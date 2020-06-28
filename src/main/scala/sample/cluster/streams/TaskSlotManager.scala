package sample.cluster.streams

import akka.NotUsed
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.typed.{ClusterSingleton, SingletonActor}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}

import scala.concurrent.duration._
import scala.util.Random

/**
 * Actor that manages "task slots" by initializing worker actors based on available slots and role assignments.
 */
object TaskSlotManager {
  sealed trait Command

  case class AssignedTaskSlotsResponse(taskSlots: TaskCoordinator.AssignedTaskSlots) extends Command

  def apply(implicit materializer: Materializer): Behavior[Command] = Behaviors.setup { ctx =>
    // TODO: read from config
    val maxSlots = 2

    val singletonManager = ClusterSingleton(ctx.system)
    val coordinatorProxy = singletonManager.init(
      SingletonActor(Behaviors.supervise(TaskCoordinator.apply()).onFailure(SupervisorStrategy.restart), "TaskCoordinator")
    )

    // message the task coordinator to get the task assignment for the current manager.
    coordinatorProxy ! TaskCoordinator.RequestTaskAssignment(
      maxSlots, ctx.messageAdapter[TaskCoordinator.AssignedTaskSlots](AssignedTaskSlotsResponse)
    )

    Behaviors.receive {
      case (ctx, AssignedTaskSlotsResponse(assigned)) =>
        ctx.log.info(s"Assigned roles: ${assigned.slots}")

        assigned.slots.foreach {
          case "data-ingress" =>
            val source = Source
              .tick(1 second, 1 second, NotUsed)
              .map { _ =>
                Random.nextInt(100000)
              }
              .mapMaterializedValue(_ => NotUsed)

            // TODO: monitor the child actor (upon termination, unregister?)
            val ingressProcessor = ctx.spawnAnonymous(DataIngressProcessor.apply(source))
            ctx.system.receptionist ! Receptionist.register(DataIngressProcessor.serviceKey, ingressProcessor)
          case "data-processor" =>
            val actorName = "DataProcessor-" + Random.nextInt(100000)
            val sink = Sink.foreach[Int] { int => println(s"[$actorName] Received data: $int") }.mapMaterializedValue(_ => NotUsed)

            // TODO: monitor the child actor (upon termination, unregister?)
            val processor = ctx.spawn(DataProcessor(sink), actorName)

            ctx.system.receptionist ! Receptionist.register(DataProcessor.serviceKey, processor)
          case invalidRole =>
            ctx.log.error(s"Unrecogized role: $invalidRole")
        }

        Behaviors.same
    }
  }
}
