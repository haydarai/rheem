package org.qcri.rheem.experiment.tpch;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.qcri.rheem.experiment.ExperimentController;
import org.qcri.rheem.experiment.ExperimentException;
import org.qcri.rheem.experiment.Implementation;
import org.qcri.rheem.experiment.RheemExperiment;
import org.qcri.rheem.experiment.tpch.query1.Q1;
import org.qcri.rheem.experiment.tpch.query1.Q1FlinkImplementation;
import org.qcri.rheem.experiment.tpch.query1.Q1JavaImplementation;
import org.qcri.rheem.experiment.tpch.query1.Q1RheemImplementation;
import org.qcri.rheem.experiment.tpch.query1.Q1SparkImplementation;
import org.qcri.rheem.experiment.tpch.query3.Q3;
import org.qcri.rheem.experiment.tpch.query5.Q5;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

public class TPCH extends RheemExperiment {


    @Override
    public void addOptions(Options options) {
        addTablesLocation(options);

        Option output_file_option = new Option(
                "o",
                "output_file",
                true,
                "the location of the output file that will use for the Tpch"
        );
        output_file_option.setRequired(true);
        options.addOption(output_file_option);


        Option query_option = new Option(
                "q",
                "query",
                true,
                "Query that will be executed for this test of tpch"
        );
        query_option.setRequired(true);
        options.addOption(query_option);
    }

    @Override
    public Implementation buildImplementation(ExperimentController controller) {

        RheemParameters parameters = new RheemParameters();

        parameters.addParameter("lineitem", new FileParameter(controller.getValue("lineitem")));
        parameters.addParameter("orders", new FileParameter(controller.getValue("orders")));
        parameters.addParameter("customer", new FileParameter(controller.getValue("customer")));

        RheemResults results = new RheemResults();

        results.addContainerOfResult("output", new FileResult(controller.getValue("output_file")));

        String query = controller.getValue("query").toUpperCase();

        Query query_implementation;
        switch (query){
            case "Q1":
                query_implementation = new Q1(controller);
                break;
            case "Q3":
                query_implementation = new Q3(controller);
                break;
            case "Q5":
                query_implementation = new Q5(controller);
                break;
            default:
                throw new ExperimentException(
                    String.format(
                        "The query %s it's not valid for experiment %s",
                        query,
                        this.getClass().getSimpleName()
                    )
                );
        }

        return query_implementation.buildImplementation(controller, parameters, results);
    }


    private void addTablesLocation(Options options){
        Option lineitem_option = new Option(
                "lineitem",
                "lineitem_file",
                true,
                "The location of the lineitem for the test of tpch"
        );
        lineitem_option.setRequired(true);
        options.addOption(lineitem_option);

        Option orders_option = new Option(
                "orders",
                "orders_file",
                true,
                "The location of the orders for the test of tpch"
        );
        orders_option.setRequired(true);
        options.addOption(orders_option);

        Option customer_option = new Option(
                "customer",
                "customer_file",
                true,
                "The location of the customer for the test of tpch"
        );
        customer_option.setRequired(true);
        options.addOption(customer_option);
    }

}
