package sample.cluster

import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import io.circe.Decoder
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import shapeless.tag
import shapeless.tag.@@

package streams {

  import akka.actor.PoisonPill
  import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityRef}

  case class ResourceConstraint(maxNr: Int, maxNrPerNode: Option[Int])
  case class TaskResources(taskParallelism: Map[String, ResourceConstraint])

  case class EntityTypeHelper[T](entityTypeKey: EntityTypeKey[T], initializeCommand: T) {
    def spawnEntity(sharding: ClusterSharding, entityId: EntityId): Unit = {
      sharding.entityRefFor(entityTypeKey, entityId) ! initializeCommand
    }

    def killEntity(sharding: ClusterSharding, entityId: EntityId): Unit = {
      sharding.entityRefFor(entityTypeKey, entityId).asInstanceOf[EntityRef[PoisonPill]] ! PoisonPill
    }
  }

  sealed trait EntityIdTag
  object EntityId {
    @inline def apply(id: String) = tag[EntityIdTag](id)
  }
}

package object streams {
  implicit val decodingConfig = io.circe.generic.extras.Configuration.default.withKebabCaseMemberNames

  implicit val resourceConstraintDecoder: Decoder[ResourceConstraint] = deriveConfiguredDecoder
  implicit val taskResourcesDecoder: Decoder[TaskResources] = deriveConfiguredDecoder

  type EntityId = String @@ EntityIdTag
}
