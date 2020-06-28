package sample.cluster.streams

import akka.NotUsed
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.{Materializer, SourceRef}
import akka.stream.scaladsl.{MergeHub, Sink}

import scala.util.{Failure, Success}

object DataProcessor {
  sealed trait Command
  case class ReceptionistListingResponse(listing: Receptionist.Listing) extends Command

  object StreamReferenceResponse {
    def fromStreamReference(reference: DataIngressProcessor.StreamReference): StreamReferenceResponse = {
      StreamReferenceResponse(reference.ref)
    }
  }

  case class StreamReferenceResponse(ref: SourceRef[Int]) extends Command

  val serviceKey: ServiceKey[Command] = ServiceKey[Command]("DataProcessor")

  def apply(processor: Sink[Int, NotUsed])
           (implicit materializer: Materializer): Behavior[Command] = Behaviors.setup { ctx =>

    ctx.system.receptionist ! Receptionist.Subscribe(
      DataIngressProcessor.serviceKey,
      ctx.messageAdapter[Receptionist.Listing](ReceptionistListingResponse)
    )

    // create a MergeHub so that we can merge all (remote) sources
    ctx.log.info("Starting a RunnableGraph for DataProcessor")
    val runnableGraph = MergeHub.source(perProducerBufferSize = 32).to(processor)
    val mergeHub = runnableGraph.run()

    initialized(mergeHub, Set.empty)
  }

  def initialized(sink: Sink[Int, NotUsed], seenSources: Set[ActorRef[_]])
                 (implicit materializer: Materializer): Behavior[Command] = Behaviors.receive {
    case (ctx, ReceptionistListingResponse(listing)) =>
      ctx.log.info(s"Receptionist listings: ${listing.allServiceInstances(listing.key).map(_.path.toStringWithoutAddress)}")

      val newSources = listing.allServiceInstances(listing.key.asInstanceOf[ServiceKey[DataIngressProcessor.Command]])
        .filterNot(ref => seenSources.contains(ref))

      // find all DataIngressProcessor's registered with the receptionist, and request a SourceRef
      newSources.foreach { ingressProcessor =>
        ctx.log.info(s"Requesting SourceRef from actor: ${ingressProcessor.path.toStringWithoutAddress}")
        ingressProcessor ! DataIngressProcessor.RequestStream(
          ctx.messageAdapter[DataIngressProcessor.StreamReference](StreamReferenceResponse.fromStreamReference)
        )
      }

      initialized(sink, seenSources ++ newSources)
    case (ctx, StreamReferenceResponse(ref)) =>
      ctx.log.info("Got SourceRef, adding to the MergeHub")
      // TODO: how to monitor if the resource ended?
      ref.source.runWith(sink)

      Behaviors.same
  }
}
