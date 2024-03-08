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
