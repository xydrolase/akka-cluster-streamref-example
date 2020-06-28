package sample.cluster.streams

import akka.NotUsed
import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.{Materializer, SourceRef}
import akka.stream.scaladsl.{MergeHub, Sink}

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

    // FIXME: need to handle new DataIngressProcessor by subscribing to DataIngressProcessor.serviceKey
    ctx.system.receptionist ! Receptionist.find(
      DataIngressProcessor.serviceKey,
      ctx.messageAdapter[Receptionist.Listing](ReceptionistListingResponse)
    )

    // create a MergeHub so that we can merge all (remote) sources
    ctx.log.info("Starting a RunnableGraph for DataProcessor")
    val runnableGraph = MergeHub.source(perProducerBufferSize = 32).to(processor)
    val mergeHub = runnableGraph.run()

    initialized(mergeHub)
  }

  def initialized(sink: Sink[Int, NotUsed])
                 (implicit materializer: Materializer): Behavior[Command] = Behaviors.receive {
    case (ctx, ReceptionistListingResponse(listing)) =>
      ctx.log.info(s"Receptionist listings: ${listing.allServiceInstances(listing.key).map(_.path.toStringWithoutAddress)}")

      // find all DataIngressProcessor's registered with the receptionist, and request a SourceRef
      listing.allServiceInstances(listing.key.asInstanceOf[ServiceKey[DataIngressProcessor.Command]]).foreach { ingressProcessor =>
        ctx.log.info(s"Requesting SourceRef from actor: ${ingressProcessor.path.toStringWithoutAddress}")
        ingressProcessor ! DataIngressProcessor.RequestStream(
          ctx.messageAdapter[DataIngressProcessor.StreamReference](StreamReferenceResponse.fromStreamReference)
        )
      }

      Behaviors.same
    case (ctx, StreamReferenceResponse(ref)) =>
      ctx.log.info("Got SourceRef, adding to the MergeHub")
      // TODO: how to monitor if the resource ended?
      ref.source.runWith(sink)

      Behaviors.same
  }
}
