package uk.co.gresearch.spark.dgraph.connector

case class Target(target: String) {

  val (host, port) = Option(target.split(":", 2)).map { case Array(h, p) => (h, p.toInt) }.get

  def withPort(port: Int): Target = Target(s"$host:$port")

  override def toString: String = target

}
