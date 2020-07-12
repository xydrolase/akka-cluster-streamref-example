package sample.cluster.streams

import akka.NotUsed
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.stream.{Materializer, SourceRef}
import akka.stream.scaladsl.{Keep, PartitionHub, Source, StreamRefs}
import sample.cluster.CborSerializable

object DataIngress {

  sealed trait Command extends CborSerializable
  case object Initialize extends Command
  case class RequestStream(replyTo: ActorRef[StreamReference]) extends Command

  case class StreamReference(ref: SourceRef[Int]) extends CborSerializable

  val typeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("DataIngress")
  val serviceKey: ServiceKey[Command] = ServiceKey[Command]("DataIngress")

  def initSharding(system: ActorSystem[_], source: Source[Int, NotUsed])
                  (implicit materializer: Materializer): ActorRef[ShardingEnvelope[Command]] = {
    ClusterSharding(system).init(Entity(typeKey) { entityContext =>
      apply(entityContext.entityId, source)
    })
  }

  def initialized(entityId: String, producer: Source[Int, NotUsed])
                 (implicit materializer: Materializer): Behavior[Command] = Behaviors.setup { ctx =>

    ctx.system.receptionist ! Receptionist.Register(serviceKey, ctx.self)

    val runnableGraph = producer.toMat(
      PartitionHub.sink[Int](
        (size, elem)  => math.abs(elem % size),
        startAfterNrOfConsumers = 2,
        bufferSize = 256)
    )(Keep.right)

    val partitionHub = runnableGraph.run()

    Behaviors.receive {
      case (ctx, RequestStream(replyTo)) =>
        ctx.log.info("Creating a new SourceRef from the PartitionHub.")

        val sourceRef: SourceRef[Int] = partitionHub.runWith(StreamRefs.sourceRef())

        replyTo ! StreamReference(sourceRef)

        Behaviors.same
      case _ =>
        Behaviors.same
    }
  }

  def apply(entityId: String, producer: Source[Int, NotUsed])(implicit materialzer: Materializer): Behavior[Command] = Behaviors.setup { ctx =>
    ctx.log.info(s"Initializing DataIngress: $entityId")

    Behaviors.receivePartial {
      case (ctx, Initialize) =>
        initialized(entityId, producer)
    }
  }
}
