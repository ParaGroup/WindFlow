/**************************************************************************************
 *  Copyright (c) 2020- Gabriele Mencagli and Fausto Frasca
 *  
 *  This file is part of WindFlow.
 *  
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/vers3.x/LICENSE.MIT
 *  
 *  WindFlow is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

package com.server.ServerState;

import com.server.ServerState.SpringRequest.GeneralInformations;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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

    // getStatOperator method
    public String getStatOperator(String idApp, String idOperator)
    {
        ApplicationWF applicationWF = getApp(Integer.valueOf(idApp));
        return applicationWF == null ? null: applicationWF.getOperatorStatistics(idOperator);
    }

    //GeneralInformations method
    public GeneralInformations getGeneralInformations()
    {
        return new GeneralInformations(this.serverState);
    }
}
