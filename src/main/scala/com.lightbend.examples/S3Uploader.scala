package com.lightbend.examples

import akka.actor.ActorRef
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

object S3Uploader {

  def main(args: Array[String]): Unit = {

    case class ModifiedRecord(id: String, date: Instant) {
      def toCSV: String = s"${id};${date.toString()}\n"
    }

    implicit val system: ActorSystem[RecordActor.RecordActorCommand] =
      ActorSystem(RecordActor(), "records")
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
      List(RecordActor.Record(1), RecordActor.Record(2), RecordActor.Record(3))

    val awsRegionProvider: AwsRegionProvider = () => Region.of(region)

    val awsCredentialsProvider = StaticCredentialsProvider.create(
      AwsBasicCredentials.create(accessKeyId, secretAccess)
    )

    val s3Settings = S3Settings(classicSystem)
      .withBufferType(MemoryBufferType)
      .withCredentialsProvider(awsCredentialsProvider)
      .withS3RegionProvider(awsRegionProvider)

    val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val source: Source[RecordActor.Record, NotUsed] =
        Source.fromIterator(() => records.toIterable.iterator)

      val printSink: Sink[RecordActor.Record, Future[Done]] =
        Sink.foreach[RecordActor.Record](r => println(s"Processed:$r"))

      val awsFlow: Flow[RecordActor.Record, ByteString, NotUsed] =
        Flow[RecordActor.Record]
          .map(r => ModifiedRecord(r.toString(), Instant.now()))
          .map(mr => ByteString(s"${mr.toCSV}"))

      val awsSink: Sink[ByteString, Future[MultipartUploadResult]] =
        S3.multipartUpload(bucket, bucketKey, ContentTypes.`text/csv(UTF-8)`)
          .withAttributes(S3Attributes.settings(s3Settings))

      val aws = b.add(awsFlow)
      val broadcast = b.add(Broadcast[RecordActor.Record](2))
      val print = b.add(printSink)

      source ~> broadcast ~> print
      broadcast ~> aws ~> awsSink

      ClosedShape
    })

    g.run()

    for (i <- 1 to 3) {
      system ! RecordActor.Record(i)
    }

  }

}

trait Streaming {}
// override def build(conf: ProbeConf) = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
//     import GraphDSL.Implicits._
//     import hpe.harmony.testkit.syntax._

//     val broadcast = builder.add(Broadcast[Record](2))
//       val input = source.via(encodeToRecordFlow).via(modifyFlow)
//         .log("core stream processing")
//         .withAttributes(ActorAttributes.supervisionStrategy(ProbeStreamBuilder.defaultSupervisionStrategy))

//     input     ~> broadcast

// sending records to the probe coordinator
//     broadcast ~> nexus
//     broadcast ~> encodeToBufferFlow.grouped(conf.batchSize)

// encoding records to byte arrays -> batching them -> concatenating them to a single byte array and sending them to s3
//     broadcast ~> encodeToBufferFlow.grouped(conf.batchSize).map(concat).flatMapConcat { b =>
//       logger.info("sending batch")
//       S3.putObject(conf.bucket.path.bucket, conf.bucket.path.key.removeTrailingSymbol("/") + "/" + System.currentTimeMillis() + ".json", Source.single(b), b.length, s3Headers = S3Headers.empty)
//     } ~> Sink.ignore

//       ClosedShape
//   })
// def create(nexusRef: akka.actor.typed.ActorRef[ProbeCoordinator.Command])(implicit S: ActorSystem[Nothing]): (ActorRef, ProbeStreamBuilder) = {
//     val source = Source.actorRef(
//       completionMatcher = {
//         case Done => CompletionStrategy.immediately
//       },
//       failureMatcher = PartialFunction.empty,
//       bufferSize = 100,
//       overflowStrategy = OverflowStrategy.dropHead
//     )

//     val nexus = Sink.foreach[Record] {
//       case (step, data, _) =>
//         nexusRef ! ProbeCoordinator.DelegateData(step, data.arraySn, data.timestamp)
//     }    val errors = Sink.foreach[ProcessingError] {
//       case x: ProcessingError.ModificationError => logger.error("received a modification error", x.cause)
//       case x: ProcessingError.EncodingError     => logger.error("received an encoding error", x.cause)
//     }    val (ref, input) = source.preMaterialize()    ref -> new ProbeStreamBuilder(input, nexus, new SymphonyRecordModifier, errors)
//   }
