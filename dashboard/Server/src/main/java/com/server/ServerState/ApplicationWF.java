package com.server.ServerState;

import java.util.*;
import com.google.gson.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import com.github.ggalmazor.ltdownsampling.Point;
import com.github.ggalmazor.ltdownsampling.LTThreeBuckets;

// ApplicationWF method
public class ApplicationWF
{
    private int idApp;
    private String name;
    private String graph;
    private String report;
    private String startTimeApplication;
    private String finishTimeApplication;
    private String remoteAddress;
    private boolean interrupted;
    private List<OperatorApp> operatorsHistoricalStatistics;
    private Gson gson;

    // Constructor
    public ApplicationWF(int idApp, String graph, String remoteAddress)
    {
        this.idApp = idApp;
        this.name = "Undefined";
        this.graph = addClassToSVG(graph);
        this.startTimeApplication = this.getDate();
        this.report = "unknown";
        this.interrupted = false;
        this.remoteAddress = remoteAddress;
        this.operatorsHistoricalStatistics = new Vector<>();
        this.gson = new GsonBuilder().setPrettyPrinting().create();
    }

    // getRemoteAddress method
    public String getRemoteAddress()
    {
        return remoteAddress;
    }

    // getDuration method
    public String getDuration()
    {
        String end = this.finishTimeApplication == null ? getDate() : this.finishTimeApplication;
        return calculateDuration(this.startTimeApplication, end);
    }

    // getIdApp method
    public int getIdApp()
    {
        return idApp;
    }

    // getName method
    public String getName()
    {
        return name;
    }

    // getGraph method
    public String getGraph()
    {
        return graph;
    }

    // getReport method
    public String getReport()
    {
        return report;
    }

    // getStartTimeApplication method
    public String getStartTimeApplication()
    {
        return startTimeApplication;
    }

    // getFinishTimeApplication method
    public String getFinishTimeApplication()
    {
        return finishTimeApplication;
    }

    // getInterrupted method
    public Boolean getInterrupted()
    {
        return interrupted;
    }

    // getOperatorStatistics method
    public String getOperatorStatistics(String operator_index)
    {
        OperatorApp operatorApp;
        String[] index = operator_index.split("_");
        boolean isOperatorNested;
        try {
             operatorApp = this.operatorsHistoricalStatistics.get(Integer.valueOf(index[0]));
        }
        catch (IndexOutOfBoundsException e) {
            return null;
        }
        isOperatorNested = operatorApp.getIsNested();
        if (isOperatorNested){
            try {
                operatorApp = operatorApp.getNestedReplicas().get(Integer.valueOf(index[1]));
            }
            catch (IndexOutOfBoundsException e){
                return null;
            }
        }
        if (interrupted && !operatorApp.isTerminated()) {
            operatorApp.setIsTerminated(true);
        }
        return operatorApp.getJsonHistoricalData();
    }

    // putReport method
    public void putReport(String report)
    {
        report = withoutEndCharacter(report);
        updateHistoricalStatistics(report);
        if (this.name.equals("Undefined")) {
            JsonObject jobj = this.gson.fromJson(report, JsonObject.class);
            this.name = jobj.get("PipeGraph_name").getAsString();
        }
        this.report = report;
    }

    // endApp method
    public void endApp(String report)
    {
        this.putReport(report);
        this.finishTimeApplication = getDate();
    }

    // interruptedApp method
    public void interruptedApp()
    {
        this.finishTimeApplication = getDate();
        this.interrupted = true;
    }

    // getDate method
    private String getDate()
    {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }

    // withoutEndCharacter method
    private String withoutEndCharacter(String string)
    {
        byte[] endString = string.substring(string.length()-1,string.length()).getBytes();
        int bool = Byte.compare(endString[0], (byte) 0); //check if the last character is \0
        return  bool == 0 ? string.substring(0,string.length()-1) : string;
    }

    // addClassToSVG method
    private String addClassToSVG(String graph)
    {
        String className = "class='svg-grafo' ";
        StringBuffer newGraph = new StringBuffer(withoutEndCharacter(graph));
        newGraph.insert(graph.indexOf("<svg")+5, className);
        return  newGraph.toString();
    }

    // calculateDuration method
    private String calculateDuration(String start, String end)
    {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        long second = 1000l;
        long minute = 60l * second;
        long hour = 60l * minute;
        // parsing input
        Date date1 = null;
        Date date2 = null;
        try {
            date1 = dateFormat.parse(start);
            date2 = dateFormat.parse(end);
        }
        catch (ParseException e) {
            e.printStackTrace();
        }
        // calculation
        long diff = date2.getTime() - date1.getTime();
        return String.format("%02d:%02d:%02d", diff / hour,(diff % hour) / minute,(diff % minute) / second);
    }

    // method to update historical statistics
    private void updateHistoricalStatistics(String report)
    {
        JsonObject reportJsonObject = this.gson.fromJson(report, JsonObject.class);
        JsonArray operatorsArray = reportJsonObject.get("Operators").getAsJsonArray();
        JsonObject operator;
        String operator_type;
        boolean isOperatorNested;
        for (int i = 0; i < operatorsArray.size(); i++) {
            operator = operatorsArray.get(i).getAsJsonObject();
            operator_type = operator.get("Operator_type").getAsString();
            try {
                isOperatorNested = operator.get("areNestedOPs").getAsBoolean();
            }
            catch (Exception e){
                isOperatorNested = false;
            }
            if (operator_type.contains("Win_MapReduce") || operator_type.contains("Pane_Farm")) {
                PF_WMR_Operator(operator, i, this.operatorsHistoricalStatistics);
            }
            else if (isOperatorNested) {
                nestedOperator(operator, i);
            }
            else {
                normalOperator(operator, i);
            }
        }
    }

    // normalOperator method
    private void normalOperator(JsonObject operator, int i)
    {
        JsonArray replicas = operator.get("Replicas").getAsJsonArray();;
        OperatorApp operatorApp;

        try {
            operatorApp = this.operatorsHistoricalStatistics.get(i);
        }
        catch (IndexOutOfBoundsException e) {
            operatorApp = new OperatorApp(operator.get("Operator_name").getAsString(),operator.get("Operator_type").getAsString());
            this.operatorsHistoricalStatistics.add(i,operatorApp); //add new operator to list
        }
        addNewDataReport(replicas,operatorApp,1);
        operatorApp.setIsTerminated(operator.get("isTerminated").getAsBoolean());
    }

    // nestedOperator method
    private void nestedOperator(JsonObject operator, int i)
    {
        JsonArray replicas = operator.get("Replicas").getAsJsonArray();;
        OperatorApp operatorApp;
        try {
            operatorApp = this.operatorsHistoricalStatistics.get(i);
        } catch (IndexOutOfBoundsException e){
            operatorApp = new OperatorApp(operator.get("Operator_name").getAsString(), operator.get("Operator_type").getAsString(), "isNested");
            this.operatorsHistoricalStatistics.add(i,operatorApp);
        }
        for (int j=0; j<replicas.size(); j++) {
            PF_WMR_Operator(replicas.get(j).getAsJsonObject(), j, operatorApp.getNestedReplicas());
        }

    }

    // PF_WMR_Operator method
    private void PF_WMR_Operator(JsonObject operator, int i, List<OperatorApp> operatorsList)
    {
        JsonArray replicas_1;
        JsonArray replicas_2;
        OperatorApp operatorApp;
        replicas_1 = operator.get("Replicas_1").getAsJsonArray();
        replicas_2 = operator.get("Replicas_2").getAsJsonArray();
        try{
            operatorApp = operatorsList.get(i);
        }
        catch (IndexOutOfBoundsException e){
            operatorApp = new OperatorApp(operator.get("Operator_name").getAsString(), operator.get("Operator_type").getAsString(), true);
            operatorsList.add(i,operatorApp);
        }
        addNewDataReport(replicas_1,operatorApp,1);
        addNewDataReport(replicas_2,operatorApp,2);
        operatorApp.setIsTerminated(operator.get("isTerminated").getAsBoolean());
    }

    // addNewDataReport method
    private void addNewDataReport(JsonArray replicas, OperatorApp operatorApp, int number_stage)
    {
        JsonObject replica;
        int inputs_received = 0;
        int outputs_sent = 0;
        for (int j = 0; j < replicas.size(); j++) { //Read all inputs and outputs of the operator
            replica = replicas.get(j).getAsJsonObject();
            inputs_received += replica.get("Inputs_received").getAsInt();
            outputs_sent += replica.get("Outputs_sent").getAsInt();
        }
        int actual_inputs_received = inputs_received - operatorApp.getLastInputsReceived(number_stage);
        int actual_outputs_sent = outputs_sent - operatorApp.getLastOutputsSent(number_stage);;
        // System.out.println("New Report "+actual_inputs_received+" "+actual_outputs_sent+" "+operatorApp.getOperator_name()+" "+operatorApp.getLastInputsReceived(number_stage)+" "+operatorApp.getLastOutputsSent(number_stage));
        operatorApp.putNewDataReport(number_stage, actual_inputs_received, actual_outputs_sent);
        operatorApp.updateInputsOutputs(number_stage, inputs_received, outputs_sent);
    }
}
