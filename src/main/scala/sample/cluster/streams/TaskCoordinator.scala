package sample.cluster.streams

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.ConfigFactory
import io.circe.config.syntax._
import sample.cluster.CborSerializable

import scala.util.Random


/**
 * A singleton actor that manages the task resources (number of tasks assigned/remaining etc.)
 */
object TaskCoordinator {
  sealed trait Command
  // Command sent by TaskSlotManager, to request task assignments
  case class RequestTaskAssignment(slots: Int, replyTo: ActorRef[AssignedTaskSlots]) extends Command with CborSerializable

  case class AssignedTaskSlots(slots: Seq[String]) extends CborSerializable

  def apply(): Behavior[Command] = Behaviors.setup { ctx =>
    val taskResources = ConfigFactory
      .load("streams.conf")
      .as[TaskResources]("streams")
      .fold(ex => throw ex, identity)

    val remainingTasks = taskResources.taskParallelism.map { case (task, rc) =>
      task -> rc.maxNr
    }

    initialized(taskResources, remainingTasks)
  }

  // FIXME: need to add support for rebalancing; add better algo than greedy assignment.
  def initialized(resources: TaskResources, remaining: Map[String, Int]): Behavior[Command] = Behaviors.receive {
    case (ctx, RequestTaskAssignment(n, replyTo)) =>
      val availableTasks = Random.shuffle(remaining.filter(_._2 > 0).toVector.flatMap { case (task, avail) =>
        Vector.fill(avail min resources.taskParallelism.get(task).flatMap(_.maxNrPerWorker).getOrElse(avail))(task)
      })

      val assignedTasks = if (remaining.getOrElse("data-ingress", 0) > 0) {
        Vector("data-ingress")
      } else {
        availableTasks.take(n)
      }

      ctx.log.info(s"Assigning tasks [${assignedTasks.mkString(", ")}] to ${replyTo.path.toStringWithoutAddress}")
      replyTo ! AssignedTaskSlots(assignedTasks)

      initialized(
        resources,
        // update the remaining task count based on the assigned task slots
        assignedTasks.foldLeft(remaining) { case (rem, taskAssigned) =>
          rem.updated(taskAssigned, rem.get(taskAssigned).map(n => n - 1).getOrElse(0))
        }
      )
  }
}
