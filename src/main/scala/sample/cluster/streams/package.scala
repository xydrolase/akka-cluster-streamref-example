package sample.cluster

import io.circe.Decoder
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder

package streams {
  case class ResourceConstraint(maxNr: Int, maxNrPerWorker: Option[Int])
  case class TaskResources(taskParallelism: Map[String, ResourceConstraint])
}

package object streams {
  implicit val decodingConfig = io.circe.generic.extras.Configuration.default.withKebabCaseMemberNames

  implicit val resourceConstraintDecoder: Decoder[ResourceConstraint] = deriveConfiguredDecoder
  implicit val taskResourcesDecoder: Decoder[TaskResources] = deriveConfiguredDecoder
}
