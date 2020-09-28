package com.server.ServerState.SpringRequest;

import java.util.Vector;
import com.server.ServerState.ServerState;
import com.server.ServerState.ApplicationWF;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

// GeneralInformations class
public class GeneralInformations
{
    private AtomicInteger runningApplicationsCounter;
    private AtomicInteger terminatedApplicationsCounter;
    private AtomicInteger interruptedApplicationsCounter;
    private Vector<SimpleApplicationWF> runApplications;
    private Vector<SimpleApplicationWF> termApplications;

    // Constructor
    public GeneralInformations(ServerState serverState)
    {
        this.runningApplicationsCounter = serverState.getRunningApp();
        this.terminatedApplicationsCounter = serverState.getFinishedApp();
        this.interruptedApplicationsCounter = serverState.getInterruptedAppCounter();
        this.runApplications = fillVector(serverState.getRunApplications());
        this.termApplications = fillVector(serverState.getTermApplications());
    }

    // getRunningApplicationsCounter method
    public AtomicInteger getRunningApplicationsCounter()
    {
        return runningApplicationsCounter;
    }

    // getTerminatedApplicationsCounter method
    public AtomicInteger getTerminatedApplicationsCounter()
    {
        return terminatedApplicationsCounter;
    }

    // getInterruptedApplicationsCounter method
    public AtomicInteger getInterruptedApplicationsCounter()
    {
        return interruptedApplicationsCounter;
    }

    // getRunApplications method
    public Vector<SimpleApplicationWF> getRunApplications()
    {
        return runApplications;
    }

    // getTermApplications method
    public Vector<SimpleApplicationWF> getTermApplications()
    {
        return termApplications;
    }

    // fillVector method
    private Vector<SimpleApplicationWF> fillVector(ConcurrentHashMap<Integer, ApplicationWF> app)
    {
        Vector<SimpleApplicationWF> auxApplications = new Vector<>();
        app.forEach((k,v) -> auxApplications.add(new SimpleApplicationWF(v)));
        return  auxApplications;
    }
}
