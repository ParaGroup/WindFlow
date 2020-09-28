package com.server.ServerState.SpringRequest;

import com.server.ServerState.ApplicationWF;

// SimpleApplicationWF class
public class SimpleApplicationWF
{
    private int idApp;
    private String name;
    private String startTimeApplication;
    private String finishTimeApplication;
    private String remoteAddress;
    private String duration;
    private boolean interrupted;

    // Constructor
    public SimpleApplicationWF(ApplicationWF applicationWF)
    {
        this.idApp = applicationWF.getIdApp();
        this.name = applicationWF.getName();
        this.startTimeApplication = applicationWF.getStartTimeApplication();
        this.finishTimeApplication = applicationWF.getFinishTimeApplication();
        this.interrupted = applicationWF.getInterrupted();
        this.remoteAddress = applicationWF.getRemoteAddress();
        this.duration = applicationWF.getDuration();
    }

    // getRemoteAddress method
    public String getRemoteAddress()
    {
        return remoteAddress;
    }

    // getFinishTimeApplication method
    public String getFinishTimeApplication()
    {
        return finishTimeApplication;
    }

    // getStartTimeApplication method
    public String getStartTimeApplication()
    {
        return startTimeApplication;
    }

    // getIdApp method
    public int getIdApp()
    {
        return idApp;
    }

    // getInterrupted method
    public boolean getInterrupted()
    {
        return interrupted;
    }

    // getName method
    public String getName()
    {
        return name;
    }

    // getDuration method
    public String getDuration()
    {
        return duration;
    }
}
