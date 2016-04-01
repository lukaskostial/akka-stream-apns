package reactivehub.akka.stream.apns

import akka.stream._
import akka.stream.scaladsl.Keep
import akka.stream.stage.{GraphStage, GraphStageLogic}
import akka.stream.testkit.TestPublisher.{Probe ⇒ TPP}
import akka.stream.testkit.TestSubscriber.{OnError, Probe ⇒ TSP}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.{TestKitBase, TestProbe}
import akka.util.ByteString
import io.netty.bootstrap.{Bootstrap, ServerBootstrap}
import io.netty.buffer.UnpooledByteBufAllocator
import io.netty.channel._
import io.netty.channel.group.DefaultChannelGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.ChannelInputShutdownEvent
import io.netty.channel.socket.nio.{NioServerSocketChannel, NioSocketChannel}
import io.netty.handler.codec.LineBasedFrameDecoder
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.util.CharsetUtil.UTF_8
import io.netty.util.concurrent.GlobalEventExecutor
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import reactivehub.akka.stream.apns.NettyHelpers._
import scala.reflect.ClassTag

object NettyHelpers {
  case object ChannelOpened
  case object ChannelClosed
  case object ServerReadClosed
  case class ServerDataRead(data: ByteString)
}

trait NettyHelpers {
  this: TestKitBase ⇒

  class Server(group: NioEventLoopGroup, halfClose: Boolean = true) {
    private val probe = TestProbe()

    private val channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)

    private val ch = new ServerBootstrap()
      .group(group)
      .channel(classOf[NioServerSocketChannel])
      .option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT)
      .childOption(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT)
      .childOption(ChannelOption.ALLOW_HALF_CLOSURE, Boolean.box(halfClose))
      .handler(new ChannelInitializer[NioServerSocketChannel] {
        override def initChannel(ch: NioServerSocketChannel): Unit = ()
      })
      .childHandler(new ChannelInitializer[NioSocketChannel] {
        override def initChannel(ch: NioSocketChannel): Unit = {
          val pipeline = ch.pipeline()
          pipeline.addLast(new LineBasedFrameDecoder(80))
          pipeline.addLast(new StringDecoder(UTF_8))
          pipeline.addLast(new StringEncoder(UTF_8))
          pipeline.addLast(new SimpleChannelInboundHandler[String](classOf[String], false) {
            val connectionProbe = TestProbe()
            val dataProbe = TestProbe()

            override def channelActive(ctx: ChannelHandlerContext): Unit = {
              channelGroup.add(ctx.channel())
              super.channelActive(ctx)
            }

            override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
              val connection = new ServerConnection(ch, connectionProbe, dataProbe)
              probe.ref ! connection
            }

            override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
              dataProbe.ref ! msg
              ctx.writeAndFlush(msg + "\n")
            }

            override def userEventTriggered(ctx: ChannelHandlerContext, evt: Any): Unit = {
              if (evt.isInstanceOf[ChannelInputShutdownEvent])
                connectionProbe.ref ! ServerReadClosed
              ctx.fireUserEventTriggered(evt)
            }

            override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = ()
          })
        }
      }).bind("localhost", 0).sync().channel()

    channelGroup.add(ch)

    val address = ch.localAddress().asInstanceOf[InetSocketAddress]

    def expectConnection(): ServerConnection = probe.expectMsgType[ServerConnection]

    def close(): Unit = {
      channelGroup.close().sync()
      group.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync()
    }
  }

  class ServerConnection(channel: NioSocketChannel, connectionProbe: TestProbe,
      dataProbe: TestProbe) {

    def expectData(data: String) = dataProbe.expectMsg(data)
    def expectReadClosed(): Unit = connectionProbe.expectMsg(ServerReadClosed)
    def write(data: String): Unit = channel.writeAndFlush(data)
    def shutdownOutput(): Unit = channel.shutdownOutput()
  }

  abstract class NettyStage extends GraphStage[FlowShape[String, String]] { self ⇒
    val in = Inlet[String]("in")
    val out = Outlet[String]("out")

    override val shape = FlowShape(in, out)

    val probe = TestProbe()
    def expectChannelOpened(): Unit = probe.expectMsg(ChannelOpened)
    def expectChannelClosed(): Unit = probe.expectMsg(ChannelClosed)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new NettyLogic(shape) {
        override protected def createChannel(bridge: ChannelHandler): ChannelFuture = {
          val future = self.createChannel(bridge)
          future.addListener(new ChannelFutureListener {
            override def operationComplete(f: ChannelFuture): Unit =
              if (f.isSuccess) probe.ref ! ChannelOpened
          })
          future.channel().closeFuture().addListener(new ChannelFutureListener {
            override def operationComplete(f: ChannelFuture): Unit = {
              probe.ref ! ChannelClosed
            }
          })
          future
        }
      }

    def createChannel(bridge: ChannelHandler): ChannelFuture
  }

  def withServer(f: (NettyStage, Server) ⇒ Any): Unit = withServer()(f)

  def withServer(halfClose: Boolean = true)(f: (NettyStage, Server) ⇒ Any): Unit = {
    val serverGroup = new NioEventLoopGroup()
    val clientGroup = new NioEventLoopGroup()
    try {
      val server = new Server(serverGroup)
      val stage = new NettyStage {
        override def createChannel(bridge: ChannelHandler): ChannelFuture = {
          new Bootstrap()
            .group(clientGroup)
            .channel(classOf[NioSocketChannel])
            .option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT)
            .option(ChannelOption.ALLOW_HALF_CLOSURE, Boolean.box(halfClose))
            .handler(new ChannelInitializer[NioSocketChannel] {
              override def initChannel(ch: NioSocketChannel): Unit = {
                val pipeline = ch.pipeline()
                pipeline.addLast(new LineBasedFrameDecoder(80))
                pipeline.addLast(new StringDecoder(UTF_8))
                pipeline.addLast(new StringEncoder(UTF_8))
                pipeline.addLast(bridge)
              }
            })
            .connect(server.address)
        }
      }
      f(stage, server)
    } finally {
      clientGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS)
      serverGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS)
    }
  }

  def run(stage: NettyStage)(implicit m: Materializer): (TPP[String], TSP[String]) =
    TestSource.probe[String]
      .via(stage)
      .toMat(TestSink.probe[String])(Keep.both)
      .run()

  def nettyStage(f: ChannelHandler ⇒ ChannelFuture): NettyStage = new NettyStage {
    override def createChannel(bridge: ChannelHandler): ChannelFuture = f(bridge)
  }

  implicit class PimpedTestSubscriberProbe(tsp: TSP[_]) {
    def expectErrorType[T <: Throwable](implicit ct: ClassTag[T]): T =
      tsp.expectEventPF {
        case OnError(cause) if ct.runtimeClass.isInstance(cause) ⇒ cause.asInstanceOf[T]
      }
  }
}
