package sample.cluster.streams

import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior, SupervisorStrategy}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityTypeKey}
import akka.cluster.typed.{ClusterSingleton, SingletonActor}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.util.Random

object App {
  object RootBehavior {

    def apply(): Behavior[Nothing] = Behaviors.setup[Nothing] { ctx =>
      implicit val materializer = Materializer.apply(ctx.system.classicSystem)
      val singletonManager = ClusterSingleton(ctx.system)

      val source = Source
        .tick(500.milliseconds, 500.milliseconds, NotUsed)
        .map { _ => Random.nextInt(100000) }
        .mapMaterializedValue(_ => NotUsed)

      val sink = Sink.foreach[Int] { int => println(s"Received data: $int") }
        .mapMaterializedValue(_ => NotUsed)

      DataProcessor.initSharding(ctx.system, sink)
      DataIngress.initSharding(ctx.system, source)

      val entityHelpers: Map[String, EntityTypeHelper[_]] = Map(
        "data-ingress" -> EntityTypeHelper(DataIngress.typeKey, DataIngress.Initialize),
        "data-processor" -> EntityTypeHelper(DataProcessor.typeKey, DataProcessor.Initialize)
      )

      val jobManager = singletonManager.init(
        SingletonActor(
          Behaviors.supervise(JobManager.apply(entityHelpers)).onFailure[Exception](SupervisorStrategy.restart),
          "JobManager"
        )
      )

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
