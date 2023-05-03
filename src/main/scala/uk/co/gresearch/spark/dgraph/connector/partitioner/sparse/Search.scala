package uk.co.gresearch.spark.dgraph.connector.partitioner.sparse

trait Search[T, R] {
  val strategy: SearchStrategy

  def search(step: SearchStep[T, R]): R = {
    strategy.middle(step) match {
      case Some(mid) =>
        search(step.move(mid))
      case None =>
        step.result()
    }
  }
}

case class SearchWithStrategy[T, R](override val strategy: SearchStrategy) extends Search[T, R]
