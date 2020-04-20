package org.qcri.rheem.api

import org.qcri.rheem.basic.data.Record
import org.qcri.rheem.basic.function.ProjectionDescriptor
import org.qcri.rheem.basic.operators.{JoinOperator, MapOperator}
import org.qcri.rheem.basic.types.RecordType
import org.qcri.rheem.core.function.FunctionDescriptor.SerializableFunction
import org.qcri.rheem.core.function.TransformationDescriptor
import org.qcri.rheem.core.optimizer.costs.LoadEstimator
import org.qcri.rheem.basic.data.{Tuple2 => T2}

/**
  * This class enhances the functionality of [[DataQuanta]] with [[Record]]s.
  */
class RecordDataQuanta(dataQuanta: DataQuanta[Record]) {

  implicit def planBuilder: PlanBuilder = dataQuanta.planBuilder

  /**
    * Feed this instance into a [[MapOperator]] with a [[ProjectionDescriptor]].
    *
    * @param fieldNames names of the fields to be projected
    * @param udfCpuLoad optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def projectRecords(fieldNames: Seq[String],
                     udfCpuLoad: LoadEstimator = null,
                     udfRamLoad: LoadEstimator = null): DataQuanta[Record] = {
    val mapOperator = MapOperator.createProjection(
      dataQuanta.output.getType.getDataUnitType.asInstanceOf[RecordType],
      fieldNames: _*
    )
    dataQuanta.connectTo(mapOperator, 0)
    wrap[Record](mapOperator)
  }

  def joinRecords(thisKeyUdf: SerializableFunction[Record, Object],
                  that: DataQuanta[Record],
                  thatKeyUdf: SerializableFunction[Record, Object]): DataQuanta[T2[Record, Record]] = {
    val joinOperator = new JoinOperator(
      new TransformationDescriptor(thisKeyUdf, basicDataUnitType[Record], basicDataUnitType[Object]),
      new TransformationDescriptor(thatKeyUdf, basicDataUnitType[Record], basicDataUnitType[Object])
    )
    dataQuanta.connectTo(joinOperator, 0)
    that.connectTo(joinOperator, 1)
    wrap[T2[Record, Record]](joinOperator)
  }

}
