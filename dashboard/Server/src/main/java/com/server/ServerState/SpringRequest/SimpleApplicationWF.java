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
