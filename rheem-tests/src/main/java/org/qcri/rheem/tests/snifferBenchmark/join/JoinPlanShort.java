package org.qcri.rheem.tests.snifferBenchmark.join;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple1;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.basic.operators.MapOperator;
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

public class JoinPlanShort extends SnifferBenchmarkBase {
    private Map<String, Operator> operatorPlan;
    private String customerFile;
    private String ordersFile;
    private String lineitemFile;
    private String outputFile;

    private boolean sniffer;

    public JoinPlanShort(int n_sniffers, String input, String output){
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
                "sniffer_1",
                new SnifferOperator<Tuple4, Tuple1>(Tuple4.class, Tuple1.class)
        );
        this.operatorPlan.put(
                "join_orders_lineitem",
                new JoinOperator<scala.Tuple4<Long, Long, Integer, Integer>, scala.Tuple2<Long, Double>, Long>(
                        tuple4 -> tuple4._1(),
                        tuple2 -> tuple2._1(),
                        ReflectionUtils.specify(scala.Tuple4.class),
                        ReflectionUtils.specify(scala.Tuple2.class),
                        Long.class
                )
        );

        this.operatorPlan.put(
                "sniffer_2",
                new SnifferOperator<Tuple2, Tuple1>(Tuple2.class, Tuple1.class)
        );

        this.operatorPlan.put(
                "clean_output",
                new FilterOperator<Tuple2>(
                        record -> {return false;},
                        Tuple2.class
                )
        );

        this.operatorPlan.put(
                "collect",
                new TextFileSink<Tuple2>(
                        this.outputFile,
                        record -> record.toString(),
                        Tuple2.class
                )
        );
    }

    @Override
    public RheemPlan generatePlan(){

        connect("order_source", 0, "order_identity", 0);
        connect("order_identity", 0, "order_parse", 0);
        connect("order_parse", 0, "order_filter", 0);
        connect("order_filter", 0, "order_project", 0);


        connect("lineitem_source", 0, "lineitem_identity", 0);
        connect("lineitem_identity", 0, "lineitem_parser", 0);
        connect("lineitem_parser", 0, "lineitem_filter", 0);
        connect("lineitem_filter", 0, "lineitem_projection", 0);
        connect("lineitem_projection", 0, "join_orders_lineitem", 1);


        if(this.sniffer){
            connect("order_project", 0, "sniffer_1", 0);
            connect("sniffer_1", 0, "join_orders_lineitem", 0);
            connect("join_orders_lineitem", 0, "sniffer_2", 0);
            connect("sniffer_2", 0, "clean_output", 0);
            connect("clean_output", 0, "collect", 0);
        }else {
            connect("order_project", 0, "join_orders_lineitem", 0);
            connect("join_orders_lineitem", 0, "clean_output", 0);
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
