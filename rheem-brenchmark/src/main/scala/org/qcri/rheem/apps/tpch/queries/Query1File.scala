package org.qcri.rheem.apps.tpch.queries

import de.hpi.isg.profiledb.store.model.Experiment
import org.qcri.rheem.api._
import org.qcri.rheem.apps.tpch.CsvUtils
import org.qcri.rheem.apps.tpch.data.LineItem
import org.qcri.rheem.apps.util.ExperimentDescriptor
import org.qcri.rheem.basic.data.Tuple2
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.plugin.Plugin
import org.qcri.rheem.jdbc.operators.JdbcTableSource
import org.qcri.rheem.jdbc.platform.JdbcPlatformTemplate

/**
  * Rheem implementation of TPC-H Query 1.
  *
  * {{{
  * select
  *   l_returnflag,
  *   l_linestatus,
  *   sum(l_quantity) as sum_qty,
  *   sum(l_extendedprice) as sum_base_price,
  *   sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
  *   sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
  *   avg(l_quantity) as avg_qty,
  *   avg(l_extendedprice) as avg_price,
  *   avg(l_discount) as avg_disc,
  *   count(*) as count_order
  * from
  *   lineitem
  * where
  *   l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)
  * group by
  *   l_returnflag,
  *   l_linestatus
  * order by
  *   l_returnflag,
  *   l_linestatus;
  * }}}
  */
class Query1File(plugins: Plugin*) extends ExperimentDescriptor {

  override def version = "0.1.0"

  def apply(configuration: Configuration,
            jdbcPlatform: JdbcPlatformTemplate,
            createTableSource: (String, Seq[String]) => JdbcTableSource,
            delta: Int = 90)
           (implicit experiment: Experiment) = {

    val rheemCtx = new RheemContext(configuration)
    plugins.foreach(rheemCtx.register)
    val planBuilder = new PlanBuilder(rheemCtx)
      .withJobName(s"TPC-H (${this.getClass.getSimpleName})")
      .withUdfJarsOf(classOf[Query1])
      .withExperiment(experiment)

    experiment.getSubject.addConfiguration("delta", delta)

    val lineitemFile = configuration.getStringProperty("rheem.apps.tpch.csv.lineitem")

    experiment.getSubject.addConfiguration("lineitemInput", lineitemFile)

    // Read, filter, and project the customer data.
    val _delta = delta
    planBuilder
      .readTextFile(lineitemFile)
      .withName("Read line items")
      .map(LineItem.parseCsv)
      .withName("Parse line items")

      .filter(t => t.shipDate <= CsvUtils.parseDate("1998-12-01") - _delta,
        sqlUdf = s"date(l_shipdate) <= date('1998-12-01', '- ${_delta} day')", selectivity = .25)
      .withName("Filter line items")
      //.map()
     // .projectRecords(Seq("l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_discount", "l_tax"))
     // .withName("Project line items")

      .map(record => Query1.Result(
        record.returnFlag.toString,
        record.lineStatus.toString,
        record.quantity,
        record.extendedPrice,
        record.extendedPrice * (1 - record.discount),
        record.extendedPrice * (1 - record.discount) * (1 + record.tax),
        record.quantity,
        record.extendedPrice,
        record.discount,
        1
      ))
      .withName("Calculate result fields")

      .reduceByKey(
        result => new Tuple2(result.l_returnflag, result.l_linestatus),
        (r1, r2) => Query1.Result(
          r1.l_returnflag,
          r1.l_linestatus,
          r1.sum_qty + r2.sum_qty,
          r1.sum_base_price + r2.sum_base_price,
          r1.sum_disc_price + r2.sum_disc_price,
          r1.sum_charge + r2.sum_charge,
          r1.avg_qty + r2.avg_qty,
          r1.avg_price + r2.avg_price,
          r1.avg_disc + r2.avg_disc,
          r1.count_order + r2.count_order
        )
      )
      .withName("Aggregate line items")

      .map(result => Query1.Result(
        result.l_returnflag,
        result.l_linestatus,
        result.sum_qty,
        result.sum_base_price,
        result.sum_disc_price,
        result.sum_charge,
        result.avg_qty / result.count_order,
        result.avg_price / result.count_order,
        result.avg_disc / result.count_order,
        result.count_order
      ))
      .withName("Post-process line item aggregates")
      .collect()


  }

}

