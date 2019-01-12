package org.qcri.rheem.experiment.tpch.query3;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.qcri.rheem.experiment.implementations.spark.SparkImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

final public class Q3SparkImplementation extends SparkImplementation {
    public Q3SparkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        String lineitem_file = ((FileParameter) parameters.getParameter("lineitem")).getPath();
        String customer_file = ((FileParameter) parameters.getParameter("customer")).getPath();
        String orders_file = ((FileParameter) parameters.getParameter("orders")).getPath();
        String output_file =((FileResult) results.getContainerOfResult("output")).getPath();



        // Filter all Orders with o_orderdate < 12.03.1995
;
        try {
            JavaPairRDD<Long, Customer> customers = getCustomerDataSet(this.sparkContext, customer_file)
                    .filter(c -> c.getMktsegment().equals("BUILDING"))
                    .keyBy(tuple -> tuple.getCustKey());


            JavaPairRDD<Long, Order> orders = getOrdersDataSet(this.sparkContext, orders_file)
                    .filter(
                        new Function<Order, Boolean>() {
                            private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                            private final Date date = format.parse("1995-03-12");

                            @Override
                            public Boolean call(Order o) throws Exception {
                                return format.parse(o.getOrderdate()).before(date);
                            }
                        }
                    )
                    .keyBy(tuple -> tuple.getCustKey());


        // Filter all Lineitems with l_shipdate > 12.03.1995
            JavaPairRDD<Long, Lineitem> lineitems = getLineitemDataSet(this.sparkContext, lineitem_file)
                    .filter(
                        new Function<Lineitem, Boolean>() {
                            private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                            private final Date date = format.parse("1995-03-12");

                            @Override
                            public Boolean call(Lineitem l) throws Exception {
                                return format.parse(l.getShipdate()).after(date);
                            }
                        }
                    ).keyBy(tuple -> tuple.getOrderkey());

            // Join customers with orders and package them into a ShippingPriorityItem
            JavaPairRDD<Long, ShippingPriorityItem> customerWithOrders = customers
                    .join(orders)
                    .map(
                        new Function<Tuple2<Long, Tuple2<Customer, Order>>, ShippingPriorityItem>() {
                            @Override
                            public ShippingPriorityItem call(Tuple2<Long, Tuple2<Customer, Order>> v1) throws Exception {
                                Customer c = v1._2()._1();
                                Order o = v1._2()._2();
                                return new ShippingPriorityItem(o.getOrderKey(), 0.0, o.getOrderdate(),
                                        o.getShippriority());
                            }
                        }
                    )
                    .keyBy(tuple -> tuple.getOrderkey());

            customerWithOrders
                .join(lineitems)
                .map(
                    new Function<Tuple2<Long, Tuple2<ShippingPriorityItem, Lineitem>>, ShippingPriorityItem>() {

                        @Override
                        public ShippingPriorityItem call(Tuple2<Long, Tuple2<ShippingPriorityItem, Lineitem>> v1) throws Exception {
                            ShippingPriorityItem i = v1._2()._1();
                            Lineitem l = v1._2()._2();
                            i.setRevenue(l.getExtendedprice() * (1 - l.getDiscount()));
                            return i;
                        }
                    }
                )
                .keyBy(tuple -> new Tuple3(tuple._1(), tuple._3(), tuple._4()))
                .reduceByKey(
                    new Function2<ShippingPriorityItem, ShippingPriorityItem, ShippingPriorityItem>() {
                        @Override
                        public ShippingPriorityItem call(ShippingPriorityItem v1, ShippingPriorityItem v2) throws Exception {
                            v1.setRevenue(v1.getRevenue() + v2.getRevenue());
                            return v1;
                        }
                    }
                ).saveAsTextFile(output_file);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    /**
     * Lineitem.
     */
    public static class Lineitem extends Tuple4<Long, Double, Double, String> {

        public Lineitem(Long aLong, Double aDouble, Double aDouble2, String s) {
            super(aLong, aDouble, aDouble2, s);
        }

        public Long getOrderkey() {
            return this._1();
        }

        public Double getDiscount() {
            return this._3();
        }

        public Double getExtendedprice() {
            return this._2();
        }

        public String getShipdate() {
            return this._4();
        }
    }

    /**
     * Customer.
     */
    public static class Customer extends Tuple2<Long, String> {


        public Customer(Long custkey, String mktsegment){
            super(custkey, mktsegment);
        }

        public Long getCustKey() {
            return this._1();
        }

        public String getMktsegment() {
            return this._2();
        }
    }

    /**
     * Order.
     */
    public static class Order extends Tuple4<Long, Long, String, Long> {

        public Order(Long aLong, Long aLong2, String s, Long aLong3) {
            super(aLong, aLong2, s, aLong3);
        }

        public Long getOrderKey() {
            return this._1();
        }

        public Long getCustKey() {
            return this._2();
        }

        public String getOrderdate() {
            return this._3();
        }

        public Long getShippriority() {
            return this._4();
        }
    }

    /**
     * ShippingPriorityItem.
     */
    public static class ShippingPriorityItem extends Tuple4<Long, Double, String, Long> {

        private Long orderkey;
        private Double revenue;

        public ShippingPriorityItem(Long orderkey, Double revenue,
                                    String orderdate, Long shippriority) {
            super(orderkey, revenue, orderdate, shippriority);
            orderkey = orderkey;
            revenue = revenue;
        }

        public Long getOrderkey() {
            return this.orderkey;
        }

        public void setOrderkey(Long orderkey) {
            this.orderkey = orderkey;
        }

        public Double getRevenue() {
            return this.revenue;
        }

        public void setRevenue(Double revenue) {
            this.revenue = revenue;
        }

        public String getOrderdate() {
            return this._3();
        }

        public Long getShippriority() {
            return this._4();
        }
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static JavaRDD<Lineitem> getLineitemDataSet(JavaSparkContext sc, String lineitemPath) {
        return sc.textFile(lineitemPath)
                    .map(line -> {
                        String[] vec = line.split("\\|");
                        return new Lineitem(
                            Long.parseLong(vec[0]),
                            Double.parseDouble(vec[5]),
                            Double.parseDouble(vec[6]),
                            vec[10]
                        );
                    });
    }

    private static JavaRDD<Customer> getCustomerDataSet(JavaSparkContext sc, String customerPath) {
        return sc.textFile(customerPath)
                .map(line -> {
                    String[] vec = line.split("\\|");
                    return new Customer(
                            Long.parseLong(vec[0]),
                            vec[6]
                    );
                });
    }

    private static JavaRDD<Order> getOrdersDataSet(JavaSparkContext sc, String ordersPath) {

        return sc.textFile(ordersPath)
                .map(line -> {
                    String[] vec = line.split("\\|");
                    return new Order(
                            Long.parseLong(vec[0]),
                            Long.parseLong(vec[1]),
                            vec[4],
                            Long.parseLong(vec[7])
                    );
                });
    }


}
