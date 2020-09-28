package com.server.ServerState.SpringRequest;

import com.server.ServerState.ApplicationWF;

// StatisticsApplicationWF class
public class StatisticsApplicationWF
{
    private String report;
    private String finishTimeApplication;
    private String duration;
    private boolean interrupted;

    // Constructor
    public StatisticsApplicationWF(ApplicationWF applicationWF)
    {
        this.report = applicationWF.getReport();
        this.finishTimeApplication = applicationWF.getFinishTimeApplication();
        this.interrupted = applicationWF.getInterrupted();
        this.duration = applicationWF.getDuration();
    }

    // getReport method
    public String getReport()
    {
        return report;
    }

    // getFinishTimeApplication method
    public String getFinishTimeApplication()
    {
        return finishTimeApplication;
    }

    // getInterrupted method
    public boolean getInterrupted()
    {
        return interrupted;
    }

    // getDuration method
    public String getDuration()
    {
        return duration;
    }
}
