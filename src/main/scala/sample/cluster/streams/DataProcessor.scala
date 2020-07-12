package sample.cluster.streams

import akka.NotUsed
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.stream.{Materializer, SourceRef}
import akka.stream.scaladsl.{MergeHub, Sink}
import sample.cluster.CborSerializable

object DataProcessor {
  sealed trait Command extends CborSerializable
  case object Initialize extends Command
  case class ReceptionistListingResponse(listing: Receptionist.Listing) extends Command

  val typeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("DataProcessor")
  val serviceKey: ServiceKey[Command] = ServiceKey[Command]("DataProcessor")

  object StreamReferenceResponse {
    def fromStreamReference(reference: DataIngress.StreamReference): StreamReferenceResponse = {
      StreamReferenceResponse(reference.ref)
    }
  }

  case class StreamReferenceResponse(ref: SourceRef[Int]) extends Command

  def initSharding(system: ActorSystem[_], processor: Sink[Int, NotUsed])
                  (implicit materializer: Materializer): Unit = {
    ClusterSharding(system).init(Entity(typeKey) { entityContext =>
      apply(entityContext.entityId, processor)
    })
  }

  def createEntity(clusterSharding: ClusterSharding, entityId: EntityId): Unit = {
    clusterSharding.entityRefFor(typeKey, entityId) ! Initialize
  }

  def apply(entityId: String, processor: Sink[Int, NotUsed])
           (implicit materializer: Materializer): Behavior[Command] = Behaviors.setup { ctx =>
    ctx.log.info(s"Creating DataProcessor: $entityId")

    ctx.system.receptionist ! Receptionist.Subscribe(
      DataIngress.serviceKey,
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
    case (ctx, Initialize) =>
      ctx.log.info("Got Initialize")
      Behaviors.same

    case (ctx, ReceptionistListingResponse(listing)) =>
      ctx.log.info(s"Receptionist listings: ${listing.allServiceInstances(listing.key).map(_.path.toStringWithoutAddress)}")

      val newSources = listing.allServiceInstances(listing.key.asInstanceOf[ServiceKey[DataIngress.Command]])
        .filterNot(ref => seenSources.contains(ref))

      // find all DataIngressProcessor's registered with the receptionist, and request a SourceRef
      newSources.foreach { ingressProcessor =>
        ctx.log.info(s"Requesting SourceRef from actor: ${ingressProcessor.path.toStringWithoutAddress}")
        ingressProcessor ! DataIngress.RequestStream(
          ctx.messageAdapter[DataIngress.StreamReference](StreamReferenceResponse.fromStreamReference)
        )
      }

      initialized(sink, seenSources ++ newSources)
    case (ctx, StreamReferenceResponse(ref)) =>
      ctx.log.info(s"Got SourceRef, adding to the MergeHub (${seenSources.size} total)")

      // TODO: how to monitor if the resource ended?
      ref.source.runWith(sink)

      Behaviors.same
  }
}
