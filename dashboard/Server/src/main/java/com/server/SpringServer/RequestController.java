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

package com.server.SpringServer;

import com.server.ServerState.ServerState;
import com.server.ServerState.ApplicationWF;
import org.springframework.web.bind.annotation.*;
import com.server.ServerState.SpringRequest.GeneralInformations;

// RequestController class
@RestController
public class RequestController
{
    ServerState serverState = ServerState.getInstance();

//    @CrossOrigin(origins = "http://localhost:3000")
    @GetMapping("/general")
    public GeneralInformations general(){
        return serverState.getGeneralInformations();
    }

//    @CrossOrigin(origins = "http://localhost:3000")
    @GetMapping("/get_all_app")
    public ApplicationWF getApplication(@RequestParam("id") Integer idApp)
    {
        return  serverState.getApp(idApp);
    }

//    @CrossOrigin(origins = "http://localhost:3000")
    @GetMapping("/get_historical_data")
    public String getHistoricalData(@RequestParam("idApp") String idApp, @RequestParam("idOperator") String idOperator)
    {
        return  serverState.getStatOperator(idApp,idOperator);
    }
}
