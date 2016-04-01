package reactivehub.akka.stream.apns

import akka.actor.ActorSystem
import akka.stream._
import akka.testkit.{ImplicitSender, TestKit}
import io.netty.channel.embedded.EmbeddedChannel
import java.net.SocketException
import org.scalatest._

class NettyLogicSpec(_system: ActorSystem)
    extends TestKit(_system)
    with NettyHelpers
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  def this() = this(ActorSystem("NettyLogicSpec"))

  implicit val materializer = ActorMaterializer()

  "NettyLogic" should "should fail the stage when createChannel fails" in {
    val (pub, sub) = run(nettyStage(_ ⇒ throw new IllegalStateException))

    pub.expectCancellation()
    sub.expectSubscription()
    sub.expectErrorType[IllegalStateException]
  }

  it should "fail the stage when channel connect fails" in {
    val (pub, sub) = run(nettyStage { _ ⇒
      new EmbeddedChannel().newFailedFuture(new SocketException)
    })

    sub.expectSubscription()
    pub.expectCancellation()
    sub.expectErrorType[SocketException]
  }

  it should "complete the stage and close the channel when upstream finishes (before the bridge is installed)" in {
    val stage = nettyStage(_ ⇒ new EmbeddedChannel().newSucceededFuture())
    val (pub, sub) = run(stage)

    stage.expectChannelOpened()

    pub.sendComplete()

    sub.expectSubscription()
    sub.expectComplete()
    stage.expectChannelClosed()
  }

  it should "fail the stage and close the channel when upstream fails (before the bridge is installed)" in {
    val stage = nettyStage(_ ⇒ new EmbeddedChannel().newSucceededFuture())
    val (pub, sub) = run(stage)

    stage.expectChannelOpened()

    pub.sendError(new RuntimeException)

    sub.expectSubscription()
    sub.expectErrorType[RuntimeException]
    stage.expectChannelClosed()
  }

  it should "complete the stage and close the channel when downstream cancels before initialized" in {
    val stage = nettyStage(_ ⇒ new EmbeddedChannel().newSucceededFuture())
    val (pub, sub) = run(stage)

    stage.expectChannelOpened()

    sub.cancel()

    pub.expectCancellation()
    stage.expectChannelClosed()
  }

  it should "work with an echo server" in withServer { (stage, server) ⇒
    val (pub, sub) = run(stage)

    stage.expectChannelOpened()
    server.expectConnection()

    sub.request(1)
    pub.sendNext("Hello\n")
    sub.expectNext("Hello")
  }

  it should "be able to send data" in withServer { (stage, server) ⇒
    val (pub, _) = run(stage)

    stage.expectChannelOpened()
    val connection = server.expectConnection()

    pub.sendNext("Hello\n")
    connection.expectData("Hello")
  }

  it should "be able receive data" in withServer { (stage, server) ⇒
    val (_, sub) = run(stage)

    stage.expectChannelOpened()
    val connection = server.expectConnection()

    connection.write("Hello\n")
    sub.request(1)
    sub.expectNext("Hello")
  }

  it should "continue receiving when upstream completes" in
    withServer { (stage, server) ⇒
      val (pub, sub) = run(stage)

      stage.expectChannelOpened()
      val connection = server.expectConnection()

      sub.request(2)
      pub.sendNext("Hello\n")
      sub.expectNext("Hello")

      pub.sendComplete()
      connection.expectReadClosed()

      connection.write("Hello2\n")
      sub.expectNext("Hello2")
    }

  it should "complete the stage when upstream completes and then server closes the write side" in
    withServer { (stage, server) ⇒
      val (pub, sub) = run(stage)

      stage.expectChannelOpened()
      val connection = server.expectConnection()

      sub.request(1)

      pub.sendComplete()
      connection.expectReadClosed()

      connection.write("Hello\n")
      sub.expectNext("Hello")

      connection.shutdownOutput()
      sub.expectComplete()
      stage.expectChannelClosed()
    }

  it should "complete the downstream and continue writing when server closes the write side" in
    withServer { (stage, server) ⇒
      val (pub, sub) = run(stage)

      stage.expectChannelOpened()
      val connection = server.expectConnection()

      connection.shutdownOutput()
      sub.expectSubscriptionAndComplete()

      pub.sendNext("Hello\n")
      connection.expectData("Hello")
    }

  it should "complete the stage when server closes the write side and then upstream completes" in
    withServer { (stage, server) ⇒
      val (pub, sub) = run(stage)

      stage.expectChannelOpened()
      val connection = server.expectConnection()

      connection.shutdownOutput()
      sub.expectSubscriptionAndComplete()

      pub.sendNext("Hello\n")
      connection.expectData("Hello")

      pub.sendComplete()
      stage.expectChannelClosed()
    }

  it should "complete the stage and close the channel when downstream cancels and then upstream completes" in
    withServer { (stage, server) ⇒
      val (pub, sub) = run(stage)

      stage.expectChannelOpened()
      sub.cancel()
      pub.sendComplete()
      stage.expectChannelClosed()
    }

  it should "complete the stage and close the channel when upstream completes and then downstream cancels" in
    withServer { (stage, server) ⇒
      val (pub, sub) = run(stage)

      stage.expectChannelOpened()

      pub.sendComplete()
      sub.cancel()
      stage.expectChannelClosed()
    }

  it should "fail the stage and close the channel when upstream fails" in
    withServer { (stage, server) ⇒
      val (pub, _) = run(stage)

      stage.expectChannelOpened()

      pub.sendError(new RuntimeException)
      stage.expectChannelClosed()
    }

  it should "complete the stage when server closes the connection" in
    withServer(halfClose = false) { (stage, server) ⇒
      val (pub, sub) = run(stage)

      stage.expectChannelOpened()
      server.expectConnection()

      server.close()
      pub.expectCancellation()
      sub.expectSubscriptionAndComplete()
      stage.expectChannelClosed()
    }
}
