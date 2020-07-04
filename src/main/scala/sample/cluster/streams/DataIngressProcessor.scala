package sample.cluster.streams

import akka.NotUsed
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.{Materializer, SourceRef}
import akka.stream.scaladsl.{Keep, PartitionHub, Source, StreamRefs}
import sample.cluster.CborSerializable

object DataIngressProcessor {

  sealed trait Command
  case class RequestStream(replyTo: ActorRef[StreamReference]) extends Command with CborSerializable

  case class StreamReference(ref: SourceRef[Int]) extends CborSerializable

  val serviceKey: ServiceKey[Command] = ServiceKey[Command]("DataIngressProcessor")

  def apply(producer: Source[Int, NotUsed])(implicit materialzer: Materializer): Behavior[Command] = Behaviors.setup { ctx =>
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
    }
  }
}
