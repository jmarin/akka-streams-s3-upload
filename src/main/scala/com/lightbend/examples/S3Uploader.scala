package com.lightbend.examples

import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Broadcast
import akka.stream.scaladsl.{Source, Flow, Sink}
import akka.Done
import akka.stream.CompletionStrategy
import akka.stream.OverflowStrategy
import akka.stream.ClosedShape
import akka.NotUsed
import akka.util.ByteString
import scala.concurrent.Future
import akka.stream.alpakka.s3.MultipartUploadResult
import akka.stream.alpakka.s3.scaladsl.S3
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import com.typesafe.config.ConfigFactory
import akka.stream.ActorMaterializer
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.stream.alpakka.s3.ObjectMetadata
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.impl.MemoryBuffer
import akka.stream.alpakka.s3.MemoryBufferType
import software.amazon.awssdk.regions.providers.AwsRegionProvider
import software.amazon.awssdk.regions.Region
import akka.stream.alpakka.s3.S3Attributes
import akka.http.scaladsl.model.ContentTypes
import java.time.Instant
import akka.stream.typed.scaladsl.ActorSource
import akka.actor.typed.ActorRef
import akka.stream.FlowShape
import akka.actor.typed.javadsl.Behaviors
import java.nio.file.Paths
import akka.stream.scaladsl.FileIO
import akka.stream.scaladsl.Keep
import akka.actor.typed.DispatcherSelector
import scala.concurrent.ExecutionContext
import scala.util.Success
import scala.util.Failure

object S3Uploader extends App {

  sealed trait Protocol
  case class Record(i: Int) extends Protocol
  case object Complete extends Protocol
  case class Fail(ex: Exception) extends Protocol

  case class ModifiedRecord(id: String, date: Instant) {
    def toCSV: String = s"${id};${date.toString()}\n"
  }

  implicit val system: ActorSystem[_] =
    ActorSystem(Behaviors.empty, "records")
  implicit val ec: ExecutionContext =
    system.dispatchers.lookup(DispatcherSelector.default())
  implicit val classicSystem = system.toClassic
  implicit val mat = ActorMaterializer()

  val awsConfig =
    ConfigFactory.load("application.conf").getConfig("aws")
  val accessKeyId = awsConfig.getString("access-key-id")
  val secretAccess = awsConfig.getString("secret-access-key ")
  val region = awsConfig.getString("region")
  val bucket = awsConfig.getString("bucket")

  val bucketKey = "testfile.csv"

  val awsRegionProvider: AwsRegionProvider = () => Region.of(region)

  val awsCredentialsProvider = StaticCredentialsProvider.create(
    AwsBasicCredentials.create(accessKeyId, secretAccess)
  )

  val s3Settings = S3Settings(classicSystem)
    .withBufferType(MemoryBufferType)
    .withCredentialsProvider(awsCredentialsProvider)
    .withS3RegionProvider(awsRegionProvider)

  val actorSource: Source[Protocol, ActorRef[Protocol]] =
    ActorSource.actorRef[Protocol](
      completionMatcher = {
        case Complete => CompletionStrategy.immediately
      },
      failureMatcher = {
        case Fail(ex) => ex
      },
      bufferSize = 100,
      overflowStrategy = OverflowStrategy.dropHead
    )

  val printSink: Sink[Protocol, Future[Done]] =
    Sink.foreach[Protocol](r => println(s"Processed:$r"))

  val awsFlow: Flow[Protocol, ByteString, NotUsed] =
    Flow[Protocol]
      .map(r => ModifiedRecord(r.toString(), Instant.now()))
      .map(mr => ByteString(s"${mr.toCSV}"))

  val awsSink: Sink[ByteString, Future[MultipartUploadResult]] =
    S3.multipartUpload(bucket, bucketKey, ContentTypes.`text/csv(UTF-8)`)
      .withAttributes(S3Attributes.settings(s3Settings))

  val (actorRef, source) = actorSource.preMaterialize()

  val flow: Flow[Protocol, ByteString, NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val input = b.add(Flow[Protocol])
      val bcast = b.add(Broadcast[Protocol](2))
      val aws = b.add(awsFlow)
      val print = b.add(printSink)

      input ~> bcast ~> print.in
      bcast ~> aws

      FlowShape(input.in, aws.out)

    })

  for (i <- 1 to 10) {
    actorRef ! Record(i)
  }

  actorRef ! Complete

  val stream = source.via(flow).toMat(awsSink)(Keep.right)

  val doneF: Future[MultipartUploadResult] = stream.run()

  doneF.onComplete {
    case Success(part) => println(part)
    case Failure(e)    => println(s"Stream failed with $e")
  }

}
