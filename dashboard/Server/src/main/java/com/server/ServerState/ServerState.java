package com.server.ServerState;

import java.util.Enumeration;
import com.google.gson.JsonArray;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.server.ServerState.SpringRequest.StatisticsApplicationWF;

// ServerState class
public class ServerState
{
    private static ServerState serverState = null;
    private ConcurrentHashMap<Integer,ApplicationWF> runApplications;
    private ConcurrentHashMap<Integer,ApplicationWF> termApplications;
    private AtomicInteger runningAppCounter;
    private AtomicInteger finishedAppCounter;
    private AtomicInteger interruptedAppCounter;

    // Constructor
    private ServerState()
    {
        runApplications = new ConcurrentHashMap<>();
        termApplications = new ConcurrentHashMap<>();
        runningAppCounter = new AtomicInteger(0);
        finishedAppCounter = new AtomicInteger(0);
        interruptedAppCounter = new AtomicInteger(0);
    }

    // getInstance method
    public static ServerState getInstance()
    {
        if (serverState == null) {
            serverState = new ServerState();
        }
        return serverState;
    }

    // addApplication method
    public void addApplication(int idApp, String graph, String remoteAddress)
    {
        ApplicationWF applicationWF = new ApplicationWF(idApp, graph, remoteAddress);
        runApplications.put(idApp, applicationWF);
        runningAppCounter.incrementAndGet();
    }

    // addNewReport method
    public void addNewReport(int idApp, String report)
    {
        ApplicationWF applicationWF = runApplications.get(idApp);
        applicationWF.putReport(report);
    }

    // endApplication method
    public void endApplication(int idApp, String report)
    {
        ApplicationWF applicationWF = runApplications.get(idApp);
        applicationWF.endApp(report);
        runningAppCounter.decrementAndGet();
        finishedAppCounter.incrementAndGet();
        runApplications.remove(idApp);
        termApplications.put(idApp, applicationWF);
    }

    // interruptedApplication method
    public void interruptedApplication(int idApp)
    {
        ApplicationWF applicationWF = runApplications.get(idApp);
        applicationWF.interruptedApp();
        runningAppCounter.decrementAndGet();
        interruptedAppCounter.incrementAndGet();
        runApplications.remove(idApp);
        termApplications.put(idApp, applicationWF);
    }

    // getFinishedApp method
    public AtomicInteger getFinishedApp()
    {
        return finishedAppCounter;
    }

    // getRunningApp method
    public AtomicInteger getRunningApp()
    {
        return runningAppCounter;
    }

    // getInterruptedAppCounter method
    public AtomicInteger getInterruptedAppCounter()
    {
        return interruptedAppCounter;
    }

    // getRunApplications method
    public ConcurrentHashMap<Integer, ApplicationWF> getRunApplications()
    {
        return runApplications;
    }

    // getTermApplications method
    public ConcurrentHashMap<Integer, ApplicationWF> getTermApplications()
    {
        return termApplications;
    }

    // getApp method
    public ApplicationWF getApp(Integer idApp)
    {
        ApplicationWF applicationWF = runApplications.get(idApp);
        if (applicationWF == null) {
            applicationWF = termApplications.get(idApp);
        }
        return applicationWF;
    }

    // getStatApp method
    public StatisticsApplicationWF getStatApp(Integer idApp)
    {
        ApplicationWF applicationWF = getApp(idApp);
        return applicationWF == null ? null: new StatisticsApplicationWF(applicationWF);
    }

    // getStatOperator method
    public String getStatOperator(String idApp, String idOperator)
    {
        ApplicationWF applicationWF = getApp(Integer.valueOf(idApp));
        return applicationWF == null ? null: applicationWF.getOperatorStatistics(idOperator);
    }

    // getGraphWF method
    public String getGraphWF(Integer idApp)
    {
        ApplicationWF applicationWF = getApp(idApp);
        return applicationWF.getReport();
    }
}
