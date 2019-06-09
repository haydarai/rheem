package org.qcri.rheem.tests.snifferBenchmark.join;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple1;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.basic.operators.SnifferOperator;
import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.tests.snifferBenchmark.SnifferBenchmarkBase;
import scala.Tuple4;

import java.util.HashMap;
import java.util.Map;

public class JoinPlan extends SnifferBenchmarkBase {
    private Map<String, Operator> operatorPlan;
    private String customerFile;
    private String ordersFile;
    private String lineitemFile;
    private String outputFile;

    private boolean sniffer;

    public JoinPlan(int n_sniffers, String input, String output){
        String[] vec = input.split("#");
        customerFile = vec[0];
        ordersFile = vec[1];
        lineitemFile = vec[2];
        outputFile = output;
        this.sniffer = n_sniffers > 0;
    }


    @Override
    protected Operator[] generateBasePlan() {
        this.operatorPlan = new HashMap<>();

        customerOperator();
        orderOperator();
        lineItemOperator();
        joinCustomerOrder();

        return new Operator[0];
    }

    public void orderOperator(){
        this.operatorPlan.put(
                "order_source",
                new TextFileSource(ordersFile)
        );

        this.operatorPlan.put(
                "order_identity",
                new MapOperator<String, String>(
                        line -> line,
                        String.class,
                        String.class
                )
        );

        this.operatorPlan.put(
                "order_parse",
                new MapOperator<String, Record>(
                        line -> {
                            String[] parsed = line.split("\\|");
                            String[] dates = parsed[4].split("-");

                            return new Record(
                                    Long.parseLong(parsed[0]),     // 0 - orderKey
                                    Long.parseLong(parsed[1]),     // 1 - custKey
                                    parsed[2].charAt(0),           // 2 - orderStatus
                                    Double.parseDouble(parsed[3]), // 3 - totalPrice
                                    (
                                            ( Integer.parseInt(dates[0])*365 )+
                                                    ( Integer.parseInt(dates[1])*30  )+
                                                    Integer.parseInt(dates[2])
                                    ), // 4 - orderDate
                                    parsed[5], // 5 - orderPriority
                                    parsed[6], // 6 - clerk
                                    Integer.parseInt(parsed[7]), // 7 - shipPriority
                                    parsed[8] // 8 - comment
                            );
                        },
                        String.class,
                        Record.class
                )
        );

        final int DATE = (1995*365) + (03*30) + 15;
        this.operatorPlan.put(
                "order_filter",
                new FilterOperator<Record>(
                        order -> order.getInt(4) < DATE,
                        Record.class
                )
        );

        this.operatorPlan.put(
            "order_project",
            new MapOperator<Record, scala.Tuple4>(
                order -> {
                    return new scala.Tuple4<Long, Long, Integer, Integer>(
                            order.getLong(0),
                            order.getLong(1),
                            order.getInt(4),
                            order.getInt(7)
                    );
                },
                Record.class,
                ReflectionUtils.specify(scala.Tuple4.class)
            )
        );
    }

    public void customerOperator(){
        this.operatorPlan.put(
            "custormer_source",
            new TextFileSource(
                customerFile
            )
        );

        this.operatorPlan.put(
            "custormer_identity",
            new MapOperator<String, String>(
                line -> line,
                String.class,
                String.class
            )
        );

        this.operatorPlan.put(
            "customer_parser",
            new MapOperator<String, Record>(
                line -> {
                    String[] parsed = line.split("\\|");
                    return new Record(
                        Long.parseLong(parsed[0]),     // 0 - custKey
                        parsed[1],                     // 1 - name
                        parsed[2],                     // 2 - address
                        Long.parseLong(parsed[3]),     // 3 - nationKey
                        parsed[4],                     // 4 - phone
                        Double.parseDouble(parsed[5]), // 5 - accbal
                        parsed[6].trim(),              // 6 - mktSegment
                        parsed[7]                      // 7 - comment
                    );
                },
                String.class,
                Record.class
            )
        );

        final String SEGMENT = "BUILDING";
        this.operatorPlan.put(
            "customer_filter",
            new FilterOperator<Record>(
                record -> {
                    return record.getString(6).compareToIgnoreCase(SEGMENT) == 0;
                },
                Record.class
            )
        );

        this.operatorPlan.put(
            "customer_projection",
            new MapOperator<Record, Long>(
                record -> record.getLong(0),
                Record.class,
                Long.class
            )
        );
    }

    public void lineItemOperator(){
        this.operatorPlan.put(
            "lineitem_source",
            new TextFileSource(lineitemFile)
        );

        this.operatorPlan.put(
            "lineitem_identity",
            new MapOperator<String, String>(
                line -> line,
                String.class,
                String.class
            )
        );

        this.operatorPlan.put(
            "lineitem_parser",
            new MapOperator<String, Record>(
                line -> {
                    String[] parsed = line.split("\\|");
                    String[] dates_shipDate = parsed[10].split("-");
                    String[] dates_commitDate = parsed[11].split("-");
                    String[] dates_receiptDate = parsed[12].split("-");
                    return new Record(
                        Long.parseLong(parsed[0]),// 0 - orderKey: Long,
                        Long.parseLong(parsed[1]),// 1 - partKey: Long,
                        Long.parseLong(parsed[2]),// 2 - suppKey: Long,
                        Integer.parseInt(parsed[3]),// 3 - lineNumber: Int,
                        Double.parseDouble(parsed[4]),// 4 - quantity: Double,
                        Double.parseDouble(parsed[5]),// 5 - extendedPrice: Double,
                        Double.parseDouble(parsed[6]),// 6 - discount: Double,
                        Double.parseDouble(parsed[7]),// 7 - tax: Double,
                        parsed[8].charAt(0),// 8 - returnFlag: Char,
                        parsed[9].charAt(0),// 9 - lineStatus: Char,
                        ( Integer.parseInt(dates_shipDate[0])*365 +
                          Integer.parseInt(dates_shipDate[1])*30 +
                          Integer.parseInt(dates_shipDate[2])
                        ),// 10 - shipDate: Int,
                        ( Integer.parseInt(dates_commitDate[0])*365 +
                          Integer.parseInt(dates_commitDate[1])*30 +
                          Integer.parseInt(dates_commitDate[2])
                        ),// 11 - commitDate: Int,
                        ( Integer.parseInt(dates_receiptDate[0])*365 +
                          Integer.parseInt(dates_receiptDate[1])*30 +
                          Integer.parseInt(dates_receiptDate[2])
                        ),// 12 - receiptDate: Int,
                        parsed[13],// 13 - shipInstruct: String,
                        parsed[14],// 14 - shipMode: String,
                        parsed[15]// 15 - comment: String)
                    );
                },
                String.class,
                Record.class
            )
        );

        final int DATE = (1995*365) + (03*30) + 15;
        this.operatorPlan.put(
            "lineitem_filter",
            new FilterOperator<Record>(
                record -> record.getInt(10) > DATE,
                Record.class
            )
        );

        this.operatorPlan.put(
            "lineitem_projection",
            new MapOperator<Record, scala.Tuple2>(
                record -> {
                    return new scala.Tuple2<Long, Double>(
                        record.getLong(0),
                        record.getDouble(5) *
                                (1 - record.getDouble(6))
                    );
                },
                Record.class,
                scala.Tuple2.class
            )
        );
    }

    public void joinCustomerOrder(){
        this.operatorPlan.put(
            "join_customer_orders",
            new JoinOperator<Long, scala.Tuple4<Long, Long, Integer, Integer>, Long>(
                customer -> customer,
                order -> order._2(),
                Long.class,
                ReflectionUtils.specify(scala.Tuple4.class),
                Long.class
            )
        );

        this.operatorPlan.put(
            "join_customer_orders_projection",
            new MapOperator<Tuple2<Long, scala.Tuple4>, scala.Tuple4>(
                tuple -> {
                     return tuple.getField1();
                },
                ReflectionUtils.specify(Tuple2.class),
                ReflectionUtils.specify(scala.Tuple4.class)
            )
        );

        this.operatorPlan.put(
            "join_previus_lineitem",
            new JoinOperator<scala.Tuple4<Long, Long, Integer, Integer>, scala.Tuple2<Long, Double>, Long>(
                tuple4 -> tuple4._1(),
                tuple2 -> tuple2._1(),
                ReflectionUtils.specify(scala.Tuple4.class),
                ReflectionUtils.specify(scala.Tuple2.class),
                Long.class
            )
        );

        this.operatorPlan.put(
            "join_previus_lineitem_projection",
            new MapOperator<Tuple2<scala.Tuple4, scala.Tuple2>,Record>(
                join -> {
                    return new Record(
                        join.field1._1(),
                        join.field1._2(),
                        join.field0._3(),
                        join.field0._4()
                    );
                },
                ReflectionUtils.specify(Tuple2.class),
                Record.class
            )
        );

        this.operatorPlan.put(
            "sniffer_1",
            new SnifferOperator<Record, Tuple1>(Record.class, Tuple1.class)
        );

        this.operatorPlan.put(
            "reduce_by",
            new ReduceByOperator<Record, scala.Tuple3>(
                record -> {
                    return new scala.Tuple3(
                        record.getLong(0),
                        record.getInt(2),
                        record.getInt(3)
                    );
                },
                (a, b) -> {
                    return new Record(
                        a.getLong(0),
                        a.getDouble(1) + b.getDouble(1),
                        a.getInt(2),
                        a.getInt(3)
                    );
                },
                ReflectionUtils.specify(scala.Tuple3.class),
                Record.class
            )
        );
        this.operatorPlan.put(
                "sniffer_2",
                new SnifferOperator<Record, Tuple1>(Record.class, Tuple1.class)
        );

        this.operatorPlan.put(
            "clean_output",
            new FilterOperator<Record>(
                record -> {return false;},
                Record.class
            )
        );

        this.operatorPlan.put(
            "collect",
            new TextFileSink<Record>(
                this.outputFile,
                record -> record.toString(),
                Record.class
            )
        );
    }

    @Override
    public RheemPlan generatePlan(){

        connect("order_source", 0, "order_identity", 0);
        connect("order_identity", 0, "order_parse", 0);
        connect("order_parse", 0, "order_filter", 0);
        connect("order_filter", 0, "order_project", 0);
        connect("order_project", 0, "join_customer_orders", 1);

        connect("custormer_source", 0, "custormer_identity", 0);
        connect("custormer_identity", 0, "customer_parser", 0);
        connect("customer_parser", 0, "customer_filter", 0);
        connect("customer_filter", 0, "customer_projection", 0);
        connect("customer_projection", 0, "join_customer_orders", 0);

        connect("lineitem_source", 0, "lineitem_identity", 0);
        connect("lineitem_identity", 0, "lineitem_parser", 0);
        connect("lineitem_parser", 0, "lineitem_filter", 0);
        connect("lineitem_filter", 0, "lineitem_projection", 0);
        connect("lineitem_projection", 0, "join_previus_lineitem", 1);



        connect("join_customer_orders", 0, "join_customer_orders_projection", 0);
        connect("join_customer_orders_projection", 0, "join_previus_lineitem", 0);
        connect("join_previus_lineitem", 0, "join_previus_lineitem_projection", 0);

        if(this.sniffer){
            connect("join_previus_lineitem_projection", 0, "sniffer_1", 0);
            connect("sniffer_1", 0, "reduce_by", 0);
            connect("reduce_by", 0, "sniffer_2", 0);
            connect("sniffer_2", 0, "clean_output", 0);
            connect("clean_output", 0, "collect", 0);
        }else {
            connect("join_previus_lineitem_projection", 0, "reduce_by", 0);
            connect("reduce_by", 0, "clean_output", 0);
            connect("clean_output", 0, "collect", 0);
        }

        return new RheemPlan(this.operatorPlan.get("collect"));
    }

    private void connect(String ope1, int out_port, String ope2, int in_port){
        Operator current_operator = this.operatorPlan.get(ope1);
        Operator next_operator = this.operatorPlan.get(ope2);
        current_operator.connectTo(out_port, next_operator, in_port);
    }
}
