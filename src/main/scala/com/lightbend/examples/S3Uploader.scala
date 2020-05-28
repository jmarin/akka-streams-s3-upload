package com.lightbend.examples

import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.Behavior
import akka.actor.typed.Terminated
import akka.actor.typed.ActorSystem
import java.time.Instant
import com.typesafe.config.ConfigFactory
import software.amazon.awssdk.regions.providers.AwsRegionProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.MemoryBufferType
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.ActorRef
import akka.stream.typed.scaladsl.ActorSource
import akka.stream.CompletionStrategy
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Sink
import scala.concurrent.Future
import akka.Done
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import akka.stream.alpakka.s3.MultipartUploadResult
import akka.http.scaladsl.model.ContentTypes
import akka.stream.alpakka.s3.S3Attributes
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Broadcast
import akka.stream.FlowShape
import akka.stream.Materializer
import akka.stream.OverflowStrategy

object S3Uploader2 {

  sealed trait Protocol
  case class Record(i: Int) extends Protocol
  case object Complete extends Protocol
  case class Fail(ex: Exception) extends Protocol

  case class ModifiedRecord(id: String, date: Instant) {
    def toCSV: String = s"${id};${date.toString()}\n"
  }

  val awsConfig =
    ConfigFactory.load("application.conf").getConfig("aws")
  val accessKeyId = awsConfig.getString("access-key-id")
  val secretAccess = awsConfig.getString("secret-access-key")
  val region = awsConfig.getString("region")
  val bucket = awsConfig.getString("bucket")

  val awsRegionProvider: AwsRegionProvider = () => Region.of(region)

  val awsCredentialsProvider = StaticCredentialsProvider.create(
    AwsBasicCredentials.create(accessKeyId, secretAccess)
  )

  def s3Settings(classicSystem: akka.actor.ActorSystem) = {
    S3Settings(classicSystem)
      .withBufferType(MemoryBufferType)
      .withCredentialsProvider(awsCredentialsProvider)
      .withS3RegionProvider(awsRegionProvider)
  }

  val printSink: Sink[Protocol, Future[Done]] =
    Sink.foreach[Protocol](r => println(s"Processed:$r"))

  val awsFlow: Flow[Protocol, ByteString, NotUsed] =
    Flow[Protocol]
      .map(r => ModifiedRecord(r.toString(), Instant.now()))
      .map(mr => ByteString(s"${mr.toCSV}"))

  def awsSink(
      s3Settings: S3Settings,
      fileName: String
  ): Sink[ByteString, Future[MultipartUploadResult]] =
    S3.multipartUpload(bucket, fileName, ContentTypes.`text/csv(UTF-8)`)
      .withAttributes(S3Attributes.settings(s3Settings))

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

  def s3MultiFlow(
      s3Settings: S3Settings
  )(
      implicit mat: Materializer
  ): Flow[Seq[ByteString], MultipartUploadResult, NotUsed] =
    Flow[Seq[ByteString]].mapAsync(parallelism = 4) { bs =>
      Source
        .fromIterator(() => bs.iterator)
        .runWith(awsSink(s3Settings, s"testfile_${Instant.now().toString()}"))
    }

  def main(args: Array[String]): Unit = {
    ActorSystem(S3Uploader2(), "s3UploadDemo")
  }

  def apply(): Behavior[NotUsed] =
    Behaviors.setup { context =>
      implicit val classicSystem = context.system.toClassic
      implicit val mat = ActorMaterializer()

      val s3Config = s3Settings(classicSystem)

      val actorSource: Source[Protocol, ActorRef[Protocol]] =
        ActorSource.actorRef[Protocol](
          completionMatcher = {
            case Complete => CompletionStrategy.immediately
          },
          failureMatcher = {
            case Fail(ex) => ex
          },
          bufferSize = 5,
          overflowStrategy = OverflowStrategy.dropHead
        )

      val (actorRef, source) = actorSource.preMaterialize()

      val range = for (i <- 1 to 10) yield i
      val messages = range.map(i => Record(i))

      for (i <- 1 to 10) {
        actorRef ! Record(i)
      }

      // Nr of records to group in each file
      val n = 3

      actorRef ! Complete

      val stream =
        source
          .via(flow)
          .grouped(n)
          .via(s3MultiFlow(s3Config))
          .to(Sink.foreach(println))

      stream.run()

      Behaviors.receiveSignal {
        case (_, Terminated(_)) =>
          Behaviors.stopped
      }

    }

}
