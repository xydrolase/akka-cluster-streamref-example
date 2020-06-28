package sample.cluster.streams

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import akka.stream.Materializer
import com.typesafe.config.ConfigFactory

object App {
  object RootBehavior {
    def apply(): Behavior[Nothing] = Behaviors.setup[Nothing] { ctx =>
      val materializer = Materializer.apply(ctx.system.classicSystem)

      ctx.spawn(TaskSlotManager.apply(materializer), "TaskSlotManager")

      Behaviors.empty
    }
  }

  def main(args: Array[String]): Unit = {
    val ports = {
      if (args.isEmpty)
        Seq(25251, 25252, 25253, 25254, 25255)
      else
        args.toSeq.map(_.toInt)
    }

    ports.foreach(startup)
  }

  def startup(port: Int): Unit = {
    // Override the configuration of the port
    val config = ConfigFactory.parseString(s"""
      akka.remote.artery.canonical.port=$port
      """).withFallback(ConfigFactory.load())

    // Create an Akka system
    ActorSystem[Nothing](RootBehavior(), "ClusterSystem", config)
  }
}
