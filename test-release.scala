import uk.co.gresearch.spark.dgraph.connector._

val triples = spark.read.dgraph.triples("localhost:9080")
triples.show()

if (triples.collect().size == 65) { sys.exit(0) }

sys.exit(1)

