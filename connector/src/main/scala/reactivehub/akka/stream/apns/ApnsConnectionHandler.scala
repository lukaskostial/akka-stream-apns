package reactivehub.akka.stream.apns

import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, ChannelPromise}
import io.netty.handler.codec.http2.Http2Error.PROTOCOL_ERROR
import io.netty.handler.codec.http2.Http2Exception.connectionError
import io.netty.handler.codec.http2.{AbstractHttp2ConnectionHandlerBuilder, _}
import io.netty.util.concurrent.PromiseCombiner
import io.netty.util.internal.ObjectUtil._
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import reactivehub.akka.stream.apns.ApnsConnectionHandler._
import reactivehub.akka.stream.apns.Priority.{High, Low}
import reactivehub.akka.stream.apns.ResponseUtil.parseStatusCode
import reactivehub.akka.stream.apns.StatusCode.OK
import scala.concurrent.Promise
import scala.util.{Failure, Success, Try}

private[apns] object ApnsConnectionHandler {
  object Builder {
    def apply(): Builder = new Builder()
  }

  final class Builder extends AbstractHttp2ConnectionHandlerBuilder[ApnsConnectionHandler, Builder] { self ⇒
    server(false)

    private var reasonPromise: Promise[Option[Reason]] = _

    def reasonPromise(reasonPromise: Promise[Option[Reason]]): Builder = {
      checkNotNull(reasonPromise, "reasonPromise")
      self.reasonPromise = reasonPromise
      self
    }

    private var propagateSettings = false

    def propagateSettings(propagateSettings: Boolean): Builder = {
      self.propagateSettings = propagateSettings
      self
    }

    private var unmarshaller: ResponseUnmarshaller = _

    def responseUnmarshaller(unmarshaller: ResponseUnmarshaller): Builder = {
      checkNotNull(unmarshaller, "unmarshaller")
      self.unmarshaller = unmarshaller
      self
    }

    override def frameLogger(frameLogger: Http2FrameLogger): Builder =
      super.frameLogger(frameLogger)

    override def build(): ApnsConnectionHandler = super.build()

    override def build(
      decoder: Http2ConnectionDecoder,
      encoder: Http2ConnectionEncoder,
      initialSettings: Http2Settings): ApnsConnectionHandler = {

      checkNotNull(reasonPromise, "reasonPromise")
      checkNotNull(unmarshaller, "unmarshaller")

      val handler = new ApnsConnectionHandler(decoder, encoder, initialSettings,
        propagateSettings, reasonPromise, unmarshaller)
      frameListener(new handler.Listener)
      handler
    }
  }

  val MethodPost = "POST"

  val HeaderContentLength = "content-length"
  val HeaderId = "apns-id"
  val HeaderExpiration = "apns-expiration"
  val HeaderPriority = "apns-priority"
  val HeaderTopic = "apns-topic"
  val HeaderStatus = ":status"
}

/**
  * A HTTP/2 channel handler which translates outgoing (T, Notification)
  * messages to HTTP/2 requests and incoming HTTP/2 responses to (T, Response).
  * Correlation ids of type T associate notifications to their corresponding
  * responses. If a notification does not contain the id and T is UUID, the
  * correlation id is used as apns id.
  */
private[apns] final class ApnsConnectionHandler(
  decoder: Http2ConnectionDecoder, encoder: Http2ConnectionEncoder,
  initialSettings: Http2Settings, propagateSettings: Boolean,
  reasonPromise: Promise[Option[Reason]], unmarshaller: ResponseUnmarshaller)
    extends Http2ConnectionHandler(decoder, encoder, initialSettings) {

  private val conn = connection()
  private val local = conn.local()
  private val correlationIdKey = conn.newKey()
  private val dataKey = conn.newKey()
  private val headersKey = conn.newKey()

  private var messageRead = false

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit =
    try {
      flush(ctx)
    } finally {
      discardSomeReadBytes()
      if (messageRead) {
        messageRead = false
        if (!ctx.channel().config().isAutoRead) ctx.read()
      }
      ctx.fireChannelReadComplete()
    }

  override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit =
    msg match {
      case (c: Any, n: Notification) ⇒ write(ctx, c, n, promise)
      case _                         ⇒ ctx.write(msg, promise)
    }

  private def write(ctx: ChannelHandlerContext, correlationId: Any,
    notification: Notification, promise: ChannelPromise): Unit =

    if (local.canOpenStream) {
      import notification._

      val streamId = local.incrementAndGetNextStreamId()

      val buffer = ctx.alloc().ioBuffer(payload.size)
      buffer.writeBytes(payload.toArray)

      val headers = new DefaultHttp2Headers()
        .method(MethodPost)
        .path(s"/3/device/$deviceToken")
        .addInt(HeaderContentLength, buffer.readableBytes())
        .addLong(HeaderExpiration, expiration.map(_.value).getOrElse(0L))

      id.orElse(correlationId match {
        case uuid: UUID ⇒ Some(uuid)
        case _          ⇒ None
      }).foreach(id ⇒ headers.add(HeaderId, id.toString))

      priority.foreach {
        case High ⇒ headers.addInt(HeaderPriority, 10)
        case Low  ⇒ headers.addInt(HeaderPriority, 5)
      }

      topic.foreach(topic ⇒ headers.add(HeaderTopic, topic))

      val headersPromise = ctx.newPromise()
      encoder.writeHeaders(ctx, streamId, headers, 0, false, headersPromise)

      val dataPromise = ctx.newPromise()
      encoder.writeData(ctx, streamId, buffer, 0, true, dataPromise)

      val promiseCombiner = new PromiseCombiner()
      promiseCombiner.addAll(headersPromise, dataPromise)
      promiseCombiner.finish(promise)

      val stream = conn.stream(streamId)
      stream.setProperty[Any](correlationIdKey, correlationId)
    } else ctx.close()

  class Listener extends Http2EventAdapter {
    override def onDataRead(ctx: ChannelHandlerContext, streamId: Int,
      data: ByteBuf, padding: Int, endOfStream: Boolean): Int = {

      val stream = connection.stream(streamId)
      val builder = stream.getProperty[StringBuilder](dataKey) match {
        case null ⇒ StringBuilder.newBuilder.append(data.toString(UTF_8))
        case b    ⇒ b.append(data.toString(UTF_8))
      }

      if (endOfStream) {
        val cid = stream.getProperty[Any](correlationIdKey)
        val headers = stream.getProperty[Http2Headers](headersKey)
        fireChannelRead(ctx, stream, cid, headers, Some(builder.toString()))
      } else stream.setProperty(dataKey, builder)

      data.readableBytes + padding
    }

    override def onHeadersRead(ctx: ChannelHandlerContext, streamId: Int,
      headers: Http2Headers, streamDependency: Int, weight: Short,
      exclusive: Boolean, padding: Int, endStream: Boolean): Unit =
      onHeadersRead(ctx, streamId, headers, padding, endStream)

    override def onHeadersRead(ctx: ChannelHandlerContext, streamId: Int,
      headers: Http2Headers, padding: Int, endStream: Boolean): Unit = {

      val stream = connection.stream(streamId)
      val newHeaders = stream.getProperty[Http2Headers](headersKey) match {
        case null ⇒ new DefaultHttp2Headers().add(headers)
        case prev ⇒ prev.add(headers)
      }

      if (endStream) {
        val cid = stream.getProperty[Any](correlationIdKey)
        val builder = Option(stream.getProperty[StringBuilder](dataKey))
        fireChannelRead(ctx, stream, cid, newHeaders, builder.map(_.toString))
      } else stream.setProperty(headersKey, newHeaders)
    }

    override def onSettingsRead(ctx: ChannelHandlerContext, settings: Http2Settings): Unit =
      if (propagateSettings) ctx.fireChannelRead(settings)

    override def onGoAwayRead(ctx: ChannelHandlerContext, lastStreamId: Int,
      errorCode: Long, debugData: ByteBuf): Unit = {

      val body = extractBody(debugData.toString(UTF_8))
      reasonPromise.success(Some(body.reason))
    }

    override def onStreamRemoved(stream: Http2Stream): Unit =
      removeStream(stream)

    private def removeStream(stream: Http2Stream): Unit = {
      stream.removeProperty[Any](correlationIdKey)
      stream.removeProperty[Http2Headers](headersKey)
      stream.removeProperty[StringBuilder](dataKey)
    }

    private def fireChannelRead(ctx: ChannelHandlerContext, stream: Http2Stream,
      correlationId: Any, headers: Http2Headers, data: Option[String]): Unit = {

      messageRead = true
      removeStream(stream)

      (extractStatusCode(headers), data) match {
        case (OK, _) ⇒
          ctx.fireChannelRead(correlationId → Response.Success)

        case (sc, Some(str)) ⇒
          val body = extractBody(str)
          val failure = Response.Failure(sc, body.reason, body.timestamp)
          ctx.fireChannelRead(correlationId → failure)

        case (sc, None) if sc.isFailure ⇒
          throw error("DATA frame not received")
      }
    }

    private def extractStatusCode(headers: Http2Headers): StatusCode = {
      if (!headers.contains(HeaderStatus))
        throw error(s"Header $HeaderStatus missing")

      parseStatusCode(headers.status().toString.toInt) match {
        case Some(statusCode) ⇒ statusCode
        case _ ⇒
          throw error(s"Header $HeaderStatus does not contain a valid status code")
      }
    }

    private def extractBody(str: String): ResponseBody =
      Try(unmarshaller.read(str)) match {
        case Success(body) ⇒ body
        case Failure(cause) ⇒
          throw error(cause, "Body does not contain a valid JSON")
      }

    private def error(cause: Throwable, msg: String): Http2Exception =
      connectionError(PROTOCOL_ERROR, cause, msg)

    private def error(msg: String): Http2Exception =
      connectionError(PROTOCOL_ERROR, msg)
  }
}
