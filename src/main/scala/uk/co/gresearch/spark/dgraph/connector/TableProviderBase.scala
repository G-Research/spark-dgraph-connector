package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.sources.DataSourceRegister

trait TableProviderBase extends TableProvider with DataSourceRegister with TargetsConfigParser
