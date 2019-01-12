package org.qcri.rheem.experiment.tpch.query3;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.qcri.rheem.experiment.implementations.flink.FlinkImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Q3FlinkImplementation extends FlinkImplementation {
    public Q3FlinkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {

        String lineitem_file = ((FileParameter) parameters.getParameter("lineitem")).getPath();
        String customer_file = ((FileParameter) parameters.getParameter("customer")).getPath();
        String orders_file = ((FileParameter) parameters.getParameter("orders")).getPath();
        String output_file =((FileResult) results.getContainerOfResult("output")).getPath();


        // get input data
        DataSet<Lineitem> lineitems = getLineitemDataSet(this.env, lineitem_file);
        DataSet<Customer> customers = getCustomerDataSet(this.env, customer_file);
        DataSet<Order> orders = getOrdersDataSet(this.env, orders_file);

        // Filter market segment "AUTOMOBILE"
        customers = customers.filter(
                new FilterFunction<Customer>() {
                    @Override
                    public boolean filter(Customer c) {
                        return c.getMktsegment().equals("BUILDING");
                    }
                });

        // Filter all Orders with o_orderdate < 12.03.1995
        try {
            orders = orders.filter(
                    new FilterFunction<Order>() {
                        private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                        private final Date date = format.parse("1995-03-12");

                        @Override
                        public boolean filter(Order o) throws ParseException {
                            return format.parse(o.getOrderdate()).before(date);
                        }
                    });
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Filter all Lineitems with l_shipdate > 12.03.1995
        try {
            lineitems = lineitems.filter(
                    new FilterFunction<Lineitem>() {
                        private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                        private final Date date = format.parse("1995-03-12");

                        @Override
                        public boolean filter(Lineitem l) throws ParseException {
                            return format.parse(l.getShipdate()).after(date);
                        }
                    });
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Join customers with orders and package them into a ShippingPriorityItem
        DataSet<ShippingPriorityItem> customerWithOrders =
                customers.join(orders).where(0).equalTo(1)
                        .with(
                                new JoinFunction<Customer, Order, ShippingPriorityItem>() {
                                    @Override
                                    public ShippingPriorityItem join(Customer c, Order o) {
                                        return new ShippingPriorityItem(o.getOrderKey(), 0.0, o.getOrderdate(),
                                                o.getShippriority());
                                    }
                                });

        // Join the last join result with Lineitems
        DataSet<ShippingPriorityItem> result =
                customerWithOrders.join(lineitems).where(0).equalTo(0)
                        .with(
                                new JoinFunction<ShippingPriorityItem, Lineitem, ShippingPriorityItem>() {
                                    @Override
                                    public ShippingPriorityItem join(ShippingPriorityItem i, Lineitem l) {
                                        i.setRevenue(l.getExtendedprice() * (1 - l.getDiscount()));
                                        return i;
                                    }
                                })
                        // Group by l_orderkey, o_orderdate and o_shippriority and compute revenue sum
                        .groupBy(0, 2, 3)
                        .aggregate(Aggregations.SUM, 1);

        // emit result

        result.writeAsText(output_file);
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    /**
     * Lineitem.
     */
    public static class Lineitem extends Tuple4<Long, Double, Double, String> {
        public Lineitem(){}

        public Lineitem(Long aLong, Double aDouble, Double aDouble2, String s) {
            super(aLong, aDouble, aDouble2, s);
        }

        public Long getOrderkey() {
            return this.f0;
        }

        public Double getDiscount() {
            return this.f2;
        }

        public Double getExtendedprice() {
            return this.f1;
        }

        public String getShipdate() {
            return this.f3;
        }
    }

    /**
     * Customer.
     */
    public static class Customer extends Tuple2<Long, String> {

        public Customer(){}

        public Long getCustKey() {
            return this.f0;
        }

        public String getMktsegment() {
            return this.f1;
        }
    }

    /**
     * Order.
     */
    public static class Order extends Tuple4<Long, Long, String, Long> {

        public Order(){}

        public Order(Long aLong, Long aLong2, String s, Long aLong3) {
            super(aLong, aLong2, s, aLong3);
        }

        public Long getOrderKey() {
            return this.f0;
        }

        public Long getCustKey() {
            return this.f1;
        }

        public String getOrderdate() {
            return this.f2;
        }

        public Long getShippriority() {
            return this.f3;
        }
    }

    /**
     * ShippingPriorityItem.
     */
    public static class ShippingPriorityItem extends Tuple4<Long, Double, String, Long> {

        public ShippingPriorityItem() {}

        public ShippingPriorityItem(Long orderkey, Double revenue,
                                    String orderdate, Long shippriority) {
            this.f0 = orderkey;
            this.f1 = revenue;
            this.f2 = orderdate;
            this.f3 = shippriority;
        }

        public Long getOrderkey() {
            return this.f0;
        }

        public void setOrderkey(Long orderkey) {
            this.f0 = orderkey;
        }

        public Double getRevenue() {
            return this.f1;
        }

        public void setRevenue(Double revenue) {
            this.f1 = revenue;
        }

        public String getOrderdate() {
            return this.f2;
        }

        public Long getShippriority() {
            return this.f3;
        }
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static DataSet<Lineitem> getLineitemDataSet(ExecutionEnvironment env, String lineitemPath) {
        return env.readCsvFile(lineitemPath)
                .fieldDelimiter("|")
                .includeFields("1000011000100000")
                .tupleType(Lineitem.class);
    }

    private static DataSet<Customer> getCustomerDataSet(ExecutionEnvironment env, String customerPath) {
        return env.readCsvFile(customerPath)
                .fieldDelimiter("|")
                .includeFields("10000010")
                .tupleType(Customer.class);
    }

    private static DataSet<Order> getOrdersDataSet(ExecutionEnvironment env, String ordersPath) {
        return env.readCsvFile(ordersPath)
                .fieldDelimiter("|")
                .includeFields("110010010")
                .tupleType(Order.class);
    }
}
