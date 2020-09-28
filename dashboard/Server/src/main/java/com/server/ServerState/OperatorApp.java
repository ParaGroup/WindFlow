package com.server.ServerState;

import java.util.List;
import java.util.Vector;
import java.math.BigDecimal;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.concurrent.atomic.AtomicInteger;
import com.github.ggalmazor.ltdownsampling.Point;
import com.github.ggalmazor.ltdownsampling.LTThreeBuckets;

// HistoricalData class
class HistoricalData
{
    private List<Point> inputs_received;
    private List<Point> outputs_sent;
    private List<Point> downsampling_inputs_received;
    private List<Point> downsampling_outputs_sent;
    private List<Point> pointsGraphInputs;
    private List<Point> pointsGraphOutputs;
    private int thresholdDownSampling = 250;
    private int updateDownsampling = 5;
    private int refreshRateData = 5;
    private AtomicInteger newDownsampling_input = new AtomicInteger(0);
    private AtomicInteger newDownsampling_output = new AtomicInteger(0);
    private AtomicInteger lastInputsSize = new AtomicInteger(0);
    private AtomicInteger lastOutputsSize = new AtomicInteger(0);

    // Constructor
    public HistoricalData()
    {
        inputs_received = new Vector<>();
        outputs_sent = new Vector<>();
        downsampling_inputs_received = new Vector<>();
        downsampling_outputs_sent = new Vector<>();
        pointsGraphInputs = new Vector<>();
        pointsGraphOutputs = new Vector<>();
    }

    // put method
    public void put(int input_received, int output_sent)
    {
        Point input = new Point(new BigDecimal(this.inputs_received.size()), new BigDecimal(input_received));
        Point output = new Point((new BigDecimal(this.outputs_sent.size())), new BigDecimal(output_sent));
        this.inputs_received.add(input);
        this.outputs_sent.add(output);
    }

    // getInputs_received method
    public List<Point> getInputs_received(boolean downsampling, boolean isTerminated)
    {
        if (!downsampling) {
            return this.inputs_received; // TODO: to be removed
        }
        else {
            pointsGraphInputs = getListResponse(this.inputs_received, this.pointsGraphInputs, this.lastInputsSize, isTerminated);
            return pointsGraphInputs;
        }
    }

    // getOutputs_sent method
    public List<Point> getOutputs_sent(boolean downsampling, boolean isTerminated)
    {
        if (!downsampling) {
            return this.outputs_sent;
        }
        else{
            pointsGraphOutputs = getListResponse(this.outputs_sent, this.pointsGraphOutputs, this.lastOutputsSize, isTerminated);
            return pointsGraphOutputs;
        }
    }

    // getListResponse method
    private List<Point> getListResponse(List<Point> list, List<Point> pointsGraph, AtomicInteger lastUpdateSize, boolean isTerminated)
    {
        List<Point> response;
        if ((isTerminated && list.size()!= lastUpdateSize.get()) || list.size() - lastUpdateSize.get() >= refreshRateData) {
            if(list.size() > thresholdDownSampling) {
                response = downSampling(list);
            }
            else {
                response = new Vector<>(list);
            }
            lastUpdateSize.set(list.size());
        }
        else{
            response = pointsGraph;
        }
        return response;
    }

    // downSampling method
    private List<Point> downSampling(List<Point> list)
    {
        return LTThreeBuckets.sorted(list,thresholdDownSampling-2);
    }
}

// OperatorApp class
public class OperatorApp
{
    private String operator_name;
    private String operator_type;
    private int last_inputs_received_1;
    private int last_inputs_received_2;
    private int last_outputs_sent_1;
    private int last_outputs_sent_2;
    private HistoricalData historical_data_1;
    private HistoricalData historical_data_2;
    private List<OperatorApp> nestedReplicas;
    private boolean areNestedOps;
    private boolean isPF_WMR;
    private boolean terminated;

    // Constructor I
    public OperatorApp(String operator_name, String operator_type)
    { // Normal operators
        this.operator_name = operator_name;
        this.operator_type = operator_type;
        this.last_inputs_received_1 = 0;
        this.last_outputs_sent_1 = 0;
        this.historical_data_1 = new HistoricalData();
        this.areNestedOps = false;
        this.isPF_WMR = false;
        this.terminated = false;
    }

    // Constructor II
    public OperatorApp(String operator_name, String operator_type, boolean isPF_WMR){ // PF & WMR operators
        this.operator_name = operator_name;
        this.operator_type = operator_type;
        this.last_inputs_received_1 = 0;
        this.last_inputs_received_2 = 0;
        this.last_outputs_sent_1 = 0;
        this.last_outputs_sent_2 = 0;
        this.historical_data_1 = new HistoricalData();
        this.historical_data_2 = new HistoricalData();
        this.areNestedOps = false;
        this.isPF_WMR = true;
        this.terminated = false;
    }

    // Constructor III
    public OperatorApp(String operator_name, String operator_type, String isNested)
    { // Nested operators
        this.operator_name = operator_name;
        this.operator_type = operator_type;
        this.nestedReplicas = new Vector<>();
        this.areNestedOps = true;
        this.isPF_WMR = false;
    }

    // getOperator_name method
    public String getOperator_name()
    {
        return operator_name;
    }

    // getNestedReplicas method
    public List<OperatorApp> getNestedReplicas()
    {
        return nestedReplicas;
    }

    // getIsNested method
    public boolean getIsNested()
    {
        return this.areNestedOps;
    }

    // getOperator_type method
    public String getOperator_type()
    {
        return operator_type;
    }

    // isTerminated method
    public boolean isTerminated()
    {
        return terminated;
    }

    // getLastInputsReceived method
    public int getLastInputsReceived(int number_stage)
    {
        return number_stage == 1 ? last_inputs_received_1 : last_inputs_received_2;
    }

    // getLastOutputsSent method
    public int getLastOutputsSent(int number_stage)
    {
        return number_stage == 1 ? last_outputs_sent_1 : last_outputs_sent_2;
    }

    // putNewDataReport method
    public void putNewDataReport(int number_stage, int input_received, int output_sent)
    {
        if (number_stage == 1) {
            historical_data_1.put(input_received,output_sent);
        }
        else {
            historical_data_2.put(input_received, output_sent);
        }
    }

    // updateInputsOutputs method
    public void updateInputsOutputs(int number_stage, int inputs_received, int outputs_sent)
    {
        if (number_stage == 1) {
            this.last_inputs_received_1 = inputs_received;
            this.last_outputs_sent_1 = outputs_sent;
        }
        else {
            this.last_inputs_received_2 = inputs_received;
            this.last_outputs_sent_2 = outputs_sent;
        }
    }

    // setIsTerminated method
    public void setIsTerminated(boolean terminated)
    {
        this.terminated = terminated;
    }

    // getJsonHistoricalData method
    public String getJsonHistoricalData()
    {
        JsonObject response = new JsonObject();
        response.add("Historical_data",jsonHistoricalData(historical_data_1.getInputs_received(true, terminated), historical_data_1.getOutputs_sent(true, terminated)));
        // response.add("full",jsonHistoricalData(historical_data_1.getInputs_received(false, terminated), historical_data_1.getOutputs_sent(false, terminated))); // TODO delete first parameter
        if (isPF_WMR) {
            response.add("Historical_data_2",this.jsonHistoricalData(historical_data_2.getInputs_received(true, terminated), historical_data_2.getOutputs_sent(true, terminated)));
            // response.add("full_2",jsonHistoricalData(historical_data_2.getInputs_received(false, terminated),historical_data_2.getOutputs_sent(false, terminated)));
        }
        return new Gson().toJson(response);
    }

    // jsonHistoricalData method
    private JsonObject jsonHistoricalData(List<Point> inputs_received, List<Point> outputs_sent)
    {
        JsonObject response = new JsonObject();
        JsonArray label_inputs_array = new JsonArray();
        JsonArray label_outputs_array = new JsonArray();
        JsonArray inputs_array = new JsonArray();
        JsonArray outputs_array = new JsonArray();
        for (int i = 0; i<inputs_received.size(); i++) {
            label_inputs_array.add(inputs_received.get(i).getX());
            inputs_array.add(inputs_received.get(i).getY());
            label_outputs_array.add(outputs_sent.get(i).getX());
            outputs_array.add(outputs_sent.get(i).getY());
        }
        JsonObject inputs = new JsonObject();
        inputs.add("Label", label_inputs_array);
        inputs.add("Inputs",inputs_array);
        JsonObject outputs = new JsonObject();
        outputs.add("Label", label_outputs_array);
        outputs.add("Outputs", outputs_array);
        response.add("Inputs_received",inputs);
        response.add("Outputs_sent",outputs);
        return  response;
    }
}
