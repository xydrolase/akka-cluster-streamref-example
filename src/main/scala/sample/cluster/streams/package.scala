package sample.cluster

import io.circe.Decoder
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import shapeless.tag
import shapeless.tag.@@

package streams {

  import akka.cluster.sharding.typed.scaladsl.EntityTypeKey

  case class ResourceConstraint(maxNr: Int, maxNrPerNode: Option[Int])
  case class TaskResources(taskParallelism: Map[String, ResourceConstraint])

  case class EntityTypeInitializer[T](key: EntityTypeKey[T], initializer: T)

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
