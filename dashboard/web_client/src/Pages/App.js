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

import React from 'react';
import GridApp from '../Layout/GridApp';
import Graph from '../Components/Graph';
import InfoApp from '../Components/InfoApp';
import TableApp from '../Components/TableApp';
import conf from '../Configuration/config.json';
import { Redirect } from "react-router-dom";
import { notification } from 'antd';

export default class App extends React.Component{
  constructor(props){
    super(props);
    this.state = {
      infoAppStatic:{
        idApp: undefined,
        name: undefined, 
        startTime: undefined,
        mode: undefined,
        operatorNumber: undefined,
        threadNumber: undefined,
        backpressure: undefined,
        nonBlocking: undefined,
        threadPinning: undefined,
      },

      infoAppDynamic:{
        droppedTuples: undefined,
        finishTime: undefined,
        interrupted: undefined,
        status: undefined,
        duration: undefined,
        rss_size_kb: undefined,
      },

      graph: undefined,
      report: undefined,

      intervalID: undefined,
      getApp: true,
      redirect: false,
    }
  }

  requestData(){
    var xhr = new XMLHttpRequest();
    xhr.open("GET", `${conf.url}/get_all_app?id=${this.props.match.params.id}`, true);
    xhr.onload = function (e) {
      
      if(!this.props.serverConnection){ //change server status on top right side
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
          
          if(!xhr.responseText){ //if there is a request of the an app id that isn´t signed on server
            this.setState({redirect:true});
            return;
          }

          var json_obj = JSON.parse(xhr.responseText);
          this.setApp(json_obj);
        } 
        else {
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
    }.bind(this);

    xhr.send(null);
  }


  setApp(json_obj){
    var parse=JSON.parse(json_obj.report);
    this.setState({ 
      infoAppStatic:{
        idApp:json_obj.idApp,
        name: json_obj.name,
        startTime: json_obj.startTimeApplication,
        mode: parse.Mode,
        operatorNumber: parse.Operator_number,
        threadNumber: parse.Thread_number,
        backpressure: parse.Backpressure,
        nonBlocking: parse.Non_blocking,
        threadPinning: parse.Thread_pinning,
      },
      infoAppDynamic:{
        finishTime: json_obj.finishTimeApplication,
        interrupted: json_obj.interrupted,
        status: !json_obj.finishTimeApplication ? 'RUNNING' : 'TERMINATED',
        duration: json_obj.duration,
        rss_size_kb: parse.rss_size_kb,
        droppedTuples: parse.Dropped_tuples
      },
      graph: json_obj.graph,
      operators: parse.Operators,
    });
  }


  componentDidMount(){
    this.setState({intervalID:setInterval(this.requestData.bind(this),conf.timeout)})
  }

  
  componentWillUnmount(){
    clearInterval(this.state.intervalID);
  }


  render(){
    if(this.state.redirect) return <Redirect to="/404"/>
   
    var interrupted = this.state.infoAppDynamic.interrupted ? 'INTERRUPTED' : undefined

    return(
      <div>
      <GridApp 
        graph={<Graph svg={this.state.graph} />}
        infoApp={<InfoApp infoStatic={this.state.infoAppStatic} infoDynamic={this.state.infoAppDynamic}/>}
        table={<TableApp operators={this.state.operators} interrupted={interrupted} idApp={this.props.match.params.id} style={{boxShadow: '0 1px 2px 0 rgba(0, 0, 0, 0.2), 0 3px 10px 0 rgba(0, 0, 0, 0.19)',}}/>}
      />
      </div>
    )
  }
    
  }