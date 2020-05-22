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

object S3Uploader extends App {

  sealed trait Protocol
  case class Record(i: Int) extends Protocol

  case class ModifiedRecord(id: String, date: Instant) {
    def toCSV: String = s"${id};${date.toString()}\n"
  }

  implicit val system: ActorSystem[_] =
    ActorSystem(Behaviors.empty, "records")
  implicit val classicSystem = system.toClassic
  implicit val mat = ActorMaterializer()

  val awsConfig =
    ConfigFactory.load("application.conf").getConfig("aws")
  val accessKeyId = awsConfig.getString("access-key-id")
  val secretAccess = awsConfig.getString("secret-access-key ")
  val region = awsConfig.getString("region")
  val bucket = awsConfig.getString("bucket")

  val bucketKey = "testfile.csv"

  val records =
    List(Record(1), Record(2), Record(3))

  val awsRegionProvider: AwsRegionProvider = () => Region.of(region)

  val awsCredentialsProvider = StaticCredentialsProvider.create(
    AwsBasicCredentials.create(accessKeyId, secretAccess)
  )

  val s3Settings = S3Settings(classicSystem)
    .withBufferType(MemoryBufferType)
    .withCredentialsProvider(awsCredentialsProvider)
    .withS3RegionProvider(awsRegionProvider)

  val source: Source[Record, NotUsed] =
    Source.fromIterator(() => records.toIterable.iterator)

  val printSink: Sink[Record, Future[Done]] =
    Sink.foreach[Record](r => println(s"Processed:$r"))

  val awsFlow: Flow[Record, ByteString, NotUsed] =
    Flow[Record]
      .map(r => ModifiedRecord(r.toString(), Instant.now()))
      .map(mr => ByteString(s"${mr.toCSV}"))

  val awsSink: Sink[ByteString, Future[MultipartUploadResult]] =
    S3.multipartUpload(bucket, bucketKey, ContentTypes.`text/csv(UTF-8)`)
      .withAttributes(S3Attributes.settings(s3Settings))

  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
    import GraphDSL.Implicits._

    val aws = b.add(awsFlow)
    val broadcast = b.add(Broadcast[Record](2))
    val print = b.add(printSink)

    source ~> broadcast ~> print
    broadcast ~> aws ~> awsSink

    ClosedShape
  })

  g.run()

}
