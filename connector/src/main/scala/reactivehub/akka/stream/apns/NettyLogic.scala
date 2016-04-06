package reactivehub.akka.stream.apns

import akka.stream.FlowShape
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler}
import io.netty.channel.ChannelOption.ALLOW_HALF_CLOSURE
import io.netty.channel._
import io.netty.channel.socket.{ChannelInputShutdownEvent, SocketChannel}
import scala.collection.mutable
import scala.reflect.{ClassTag, classTag}

/**
  * A flow-shaped GraphStageLogic which writes and reads from a Netty channel.
  */
private[apns] abstract class NettyLogic[I, O: ClassTag](
  shape: FlowShape[I, O],
  readQueueHighWatermark: Int = 100,
  readQueueLowWatermark: Int = 20)
    extends GraphStageLogic(shape) {

  require(readQueueLowWatermark <= readQueueHighWatermark)

  private var channel: Channel = _

  private val queue = new mutable.Queue[O]
  private var demand = 0

  private val classO = classTag[O].runtimeClass

  override def preStart(): Unit = {
    setKeepGoing(true)
    val f = createChannel(new Bridge)
    channel = f.channel()
    f.addListener(new ConnectListener)
  }

  /**
    * Builds a new channel. When channel is initialized, the bridge should
    * be added to the end of the channel pipeline so that the flow communicates
    * with the channel.
    */
  protected def createChannel(bridge: ChannelHandler): ChannelFuture

  /**
    * Listens to the outcome of the channel connect future. If the future
    * succeeds, close future listener is installed; otherwise the stage
    * is completed with the same error.
    */
  private class ConnectListener extends ChannelFutureListener {
    override def operationComplete(f: ChannelFuture): Unit =
      if (f.isSuccess) f.channel().closeFuture().addListener(new CloseListener)
      else connectFailed(f.cause())
  }

  /**
    * Completes the stage when the channel connect future fails.
    */
  private val connectFailed = getAsyncCallback[Throwable](failStage) invoke _

  /**
    * Listens to the outcome of the channel close future. If the future
    * completes, the stage is completed.
    */
  private class CloseListener extends ChannelFutureListener {
    override def operationComplete(f: ChannelFuture): Unit = channelClosed(())
  }

  /**
    * Completes the stage when the channel closes.
    *
    * If there are any queued messages (received from the channel but not yet
    * pushed to the output) they will be emitted before the stage is completed.
    */
  private val channelClosed = getAsyncCallback[Unit](_ ⇒ emitQueue()) invoke _

  private var emitting = false

  /**
    * When channelClosed is called there still can be a pushOutput call
    * pending. It is not possible to use the OOTB emitMultiple because elements
    * delivered by the delayed pushOutput calls must be returned back to the
    * front of the queue.
    */
  private def emitQueue(): Unit = {
    if (!isClosed(shape.out) && queue.nonEmpty) {
      emitting = true
      cancel(shape.in)
      if (isAvailable(shape.out)) push(shape.out, queue.dequeue())
      if (queue.nonEmpty) {
        setHandler(shape.out, new OutHandler {
          override def onPull(): Unit = {
            push(shape.out, queue.dequeue())
            if (queue.isEmpty) completeStage()
          }
          override def onDownstreamFinish(): Unit = completeStage()
        })
      } else completeStage()
    } else completeStage()
  }

  class Bridge extends ChannelDuplexHandler with InHandler with OutHandler {
    private var ctx: ChannelHandlerContext = _

    override def handlerAdded(ctx: ChannelHandlerContext): Unit =
      if (ctx.channel.isActive) setChannelReady(ctx)

    override def channelActive(ctx: ChannelHandlerContext): Unit = {
      setChannelReady(ctx)
      ctx.fireChannelActive()
    }

    /**
      * Installs the bridge as GraphStageLogic handlers for the input and
      * output when it (as a channel handler) is ready to process events and
      * the channel is active.
      */
    private def setChannelReady(ctx: ChannelHandlerContext): Unit = {
      this.ctx = ctx
      channelReady(this)
    }

    override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit =
      if (classO.isInstance(msg)) channelReadO(ctx, msg.asInstanceOf[O])
      else ctx.fireChannelRead(msg)

    private def channelReadO(ctx: ChannelHandlerContext, msg: O): Unit =
      if (demand == 0 || queue.nonEmpty) {
        queue += msg
        if (queue.size >= readQueueHighWatermark && isAutoRead(ctx))
          setAutoRead(ctx, autoRead = false)
      } else {
        pushOutput(msg)
        demand -= 1
      }

    override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
      if (demand > 0 && !isAutoRead(ctx)) ctx.read()
      ctx.fireChannelReadComplete()
    }

    override def channelWritabilityChanged(ctx: ChannelHandlerContext): Unit = {
      tryPullInput(ctx)
      ctx.fireChannelWritabilityChanged()
    }

    /**
      * Pulls the input if the channel is writable.
      */
    private def tryPullInput(ctx: ChannelHandlerContext): Unit =
      if (ctx.channel().isWritable) pullInput(())

    override def userEventTriggered(ctx: ChannelHandlerContext, evt: Any): Unit = {
      if (evt.isInstanceOf[ChannelInputShutdownEvent]) channelReadClosed(())
      ctx.fireUserEventTriggered(evt)
    }

    override def onPush(): Unit =
      ctx.writeAndFlush(grab(shape.in)).addListener(new ChannelFutureListener {
        override def operationComplete(f: ChannelFuture): Unit = tryPullInput(ctx)
      })

    override def onUpstreamFinish(): Unit = ctx.channel() match {
      case sc: SocketChannel if canHalfClose(sc) ⇒ sc.shutdownOutput()
      case c                                     ⇒ c.close()
    }

    /**
      * Checks if the channel supports half closing and if the output is
      * still opened.
      */
    private def canHalfClose(channel: Channel): Boolean =
      channel.config().getOption(ALLOW_HALF_CLOSURE) && !isClosed(shape.out)

    override def onUpstreamFailure(ex: Throwable): Unit = fail(ex)

    override def onPull(): Unit = addDemand()

    override def onDownstreamFinish(): Unit = cancel()

    /**
      * Requests more data from the channel when the output is pulled.
      */
    def addDemand(): Unit = ctx.channel().eventLoop().execute(new Runnable {
      override def run(): Unit =
        if (ctx.channel().isOpen) {
          if (queue.nonEmpty) {
            pushOutput(queue.dequeue())
            if (queue.size < readQueueLowWatermark && !isAutoRead(ctx))
              setAutoRead(ctx, autoRead = true)
          } else {
            demand += 1
            if (!isAutoRead(ctx)) ctx.read()
          }
        }
    })

    private def isAutoRead(ctx: ChannelHandlerContext): Boolean =
      ctx.channel().config().isAutoRead

    private def setAutoRead(ctx: ChannelHandlerContext, autoRead: Boolean): Unit =
      ctx.channel().config().setAutoRead(autoRead)
  }

  /**
    * Installs the bridge as GraphStageLogic handlers for the input and
    * output, pulls the input and if the output has been pulled before,
    * adds the demand.
    */
  private val channelReady = getAsyncCallback[Bridge] { bridge ⇒
    setHandlers(shape.in, shape.out, bridge)
    if (!hasBeenPulled(shape.in)) tryPull(shape.in)
    if (isAvailable(shape.out)) bridge.addDemand()
  } invoke _

  /**
    * Pushes out data received from the channel.
    */
  private val pushOutput = getAsyncCallback[O] { elem ⇒
    if (emitting) queue.+=:(elem) else push(shape.out, elem)
  } invoke _

  /**
    * Requests more data from the input.
    */
  private val pullInput = getAsyncCallback[Unit] { _ ⇒
    if (!hasBeenPulled(shape.in)) tryPull(shape.in)
  } invoke _

  /**
    * Handles when the read side of the channel closes. If the input has
    * already been closed, closes the channel and completes the stage;
    * otherwise completes only the output.
    */
  private val channelReadClosed = getAsyncCallback[Unit] { _ ⇒
    if (isClosed(shape.in)) channel.close() else complete(shape.out)
  } invoke _

  /**
    * Closes the output port with an error and terminates the channel when
    * upstream fails.
    */
  private def fail(cause: Throwable): Unit = {
    fail(shape.out, cause)
    channel.close()
  }

  /**
    * Closes the input port and terminates the channel when downstream finishes.
    */
  private def cancel(): Unit = {
    cancel(shape.in)
    channel.close()
  }

  /**
    * Initial handler for the input until the bridge is installed. While in
    * the initial state, input is never pulled and thus never pushed.
    * When upstream finishes or fails, the signal is propagated to the output
    * and the (semi-initialized) channel closes. After the channel is closed,
    * the stage completes.
    */
  setHandler(shape.in, new InHandler {
    override def onPush(): Unit = ()
    override def onUpstreamFinish(): Unit = channel.close()
    override def onUpstreamFailure(cause: Throwable): Unit = fail(cause)
  })

  /**
    * Initial handler for the output until the bridge is installed. When the
    * output is pulled, the signal is ignored until the bridge is installed.
    * When downstream cancels, the signal is propagated to the input and the
    * (semi-initialized) channel closes. After the channel is closed,
    * the stage completes.
    */
  setHandler(shape.out, new OutHandler {
    override def onPull(): Unit = ()
    override def onDownstreamFinish(): Unit = cancel()
  })
}
