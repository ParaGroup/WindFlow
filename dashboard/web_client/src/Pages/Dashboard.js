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

import React from 'react'
import GridDashboard from '../Layout/GridDashboard'
import Statistics from '../Components/Statistics'
import TableDashboard from '../Components/TableDashboard'
import conf from '../Configuration/config.json'
import { notification } from 'antd';

export default class Dashboard extends React.Component{
  constructor(props){
    super(props)
    this.state = {
      runningAppCounter: 0,
      terminatedAppCounter: 0,
      interruptedAppCounter: 0,
      runApp:undefined,
      termApp:undefined,
      intervalID:0,
    }
  }
  

  constructObj(key,status){
    var tempObj={
      id:key.idApp,
      name: key.name,
      start: key.startTimeApplication,
      end: !key.finishTimeApplication ? '-' : key.finishTimeApplication,
      remoteAddress: key.remoteAddress,
      status: status,
      duration: key.duration
    }

    return tempObj;
  }

  getData(){
    var xhr = new XMLHttpRequest();
    xhr.open("GET", `${conf.url}/general`, true);
    xhr.onload = function (e) {

      if(!this.props.serverConnection){
        this.props.updateServerConnection(true);
        
        notification['success']({
          message: 'Server connected',
          description: 'The dashboard has been connected to the server',
          duration:5,
          style:{
            borderRadius:5
          }
        })
      }

      if (xhr.readyState === 4) {
        if (xhr.status === 200) {
          var json_obj = JSON.parse(xhr.responseText);

          var auxRunApp = json_obj.runApplications.map((key) =>{
            return this.constructObj(key,['running']);
          });

          var auxTermApp = json_obj.termApplications.map((key) =>{
            return this.constructObj(key,[key.interrupted?'interrupted':'terminated']);
          })

          this.setState({ 
            runningAppCounter: json_obj.runningApplicationsCounter,
            terminatedAppCounter: json_obj.terminatedApplicationsCounter,
            interruptedAppCounter: json_obj.interruptedApplicationsCounter,
            runApp: auxRunApp,
            termApp:auxTermApp,
          });
          
        } else {
          console.error(xhr.statusText);
        }
      }
    }.bind(this);

    xhr.onerror = function (e) {
      if(this.props.serverConnection){
        this.props.updateServerConnection(false);
        
        notification['error']({
          message: 'Server disconnected',
          description: 'The dashboard has been disconnected from the server',
          duration:5,
          style:{
            borderRadius:5
          }
        })

      }
      
      this.stopGetData(); //TODO sistemare bene
    }.bind(this);

    xhr.send(null);

  }

  stopGetData(){
    // clearInterval(this.state.intervalID); //TODO
  }

  componentWillUnmount(){
    clearInterval(this.state.intervalID);
  }

  componentDidMount(){
    this.setState({intervalID:setInterval(this.getData.bind(this),conf.timeout)})

  }

  render(){
    return(
          <GridDashboard 
            runningApp={<Statistics title={'Running'} value={this.state.runningAppCounter} color="#186BB7"/>}
            terminatedApp={<Statistics title={'Terminated'} value={this.state.terminatedAppCounter} color="#3f8600" />}
            interruptedApp={<Statistics title={'Interrupted'} value={this.state.interruptedAppCounter} color="#E14013" />}
            tableRunApplication={<TableDashboard data={this.state.runApp} table='running' pushAppSwitcher={this.props.pushAppSwitcher} />}
            tableTermApplication={<TableDashboard data={this.state.termApp} table='terminated' pushAppSwitcher={this.props.pushAppSwitcher}/>}
          />
    )
  }
  
}