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
    def spawnEntityCreator[T](entityTypeKey: EntityTypeKey[T], command: T): (ClusterSharding, EntityId) => Unit = {
      (sharding: ClusterSharding, entityId: EntityId) => {
        sharding.entityRefFor(entityTypeKey, entityId) ! command
      }
    }

    def apply(): Behavior[Nothing] = Behaviors.setup[Nothing] { ctx =>
      implicit val materializer = Materializer.apply(ctx.system.classicSystem)
      val singletonManager = ClusterSingleton(ctx.system)

      val source = Source
        .tick(50.milliseconds, 50.milliseconds, NotUsed)
        .map { _ => Random.nextInt(100000) }
        .mapMaterializedValue(_ => NotUsed)

      val sink = Sink.foreach[Int] { int => println(s"Received data: $int") }
        .mapMaterializedValue(_ => NotUsed)

      DataProcessor.initSharding(ctx.system, sink)
      DataIngress.initSharding(ctx.system, source)

      val entityCreators: Map[String, (ClusterSharding, EntityId) => Unit] = Map(
        "data-ingress" -> spawnEntityCreator(DataIngress.typeKey, DataIngress.Initialize),
        "data-processor" -> spawnEntityCreator(DataProcessor.typeKey, DataProcessor.Initialize)
      )

      val jobManager = singletonManager.init(
        SingletonActor(
          Behaviors.supervise(JobManager.apply(entityCreators)).onFailure[Exception](SupervisorStrategy.restart),
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
