package reactivehub.akka.stream.apns

import java.lang.Integer.parseInt

/**
  * APNs device token. Tokens are either 32 or 100 bytes arrays.
  */
final class DeviceToken private[apns] (val bytes: List[Byte]) {
  override def toString: String = bytes.map("%02X" format _).mkString
}

object DeviceToken {
  def apply(bytes: Seq[Byte]): DeviceToken = {
    require(
      bytes.length == 32 || bytes.length == 100,
      "Device token must be a 32 or 100 bytes array")
    new DeviceToken(bytes.toList)
  }

  private val fmt = """\A(?:\p{XDigit}\p{XDigit})+\z""".r

  def apply(str: String): DeviceToken = {
    require(
      (str.length == 64 || str.length == 200) && fmt.findFirstIn(str).isDefined,
      "Device token must be a 64 or 200 chars hex string")
    new DeviceToken(str.sliding(2, 2).map(parseInt(_, 16).toByte).toList)
  }
}
