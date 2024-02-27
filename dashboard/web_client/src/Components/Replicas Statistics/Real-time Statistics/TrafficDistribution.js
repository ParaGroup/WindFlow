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
import {Bar} from 'react-chartjs-2';
import _ from 'lodash';
import GridTrafficDistribution from '../../../Layout/GridTrafficDistribution'



export default class TrafficDistribution extends React.Component{
  constructor(props){
    super(props)
    this.state ={
      previousDataLastSecond: {
        overall_input_received: 0,
        replica_input_received:[]
      },
      previousDataLastSecond_2: {
        overall_input_received: 0,
        replica_input_received:[]
      },
    }
    
    this.optionChartOverall = {
      title: {
        display: true,
        text: 'Traffic distribution per replica (overall elapsed time)',
        fontSize: 14,
        // fontStyle: 'normal'
      },
      scales: {
        yAxes: [{
          ticks: {
            beginAtZero:true,
            maxTicksLimit:5,
            callback: function(value, index, values) {
              return value + '%';
            }
          }
        }],
        xAxes: [{
          gridLines: {
            display: false,
            drawTicks: false
          },
          ticks:{
            padding: 5
          }
        }]
      },
      legend: {
        display: false,
      },
      tooltips: {
        xPadding:10,
        titleFontSize:0,
        titleMarginBottom:0,
        bodyFontSize: 13,
        displayColors: false,
      },
      maintainAspectRatio: false
    }

    this.optionChartOverall_2 = _.cloneDeep(this.optionChartOverall);
  
    this.optionChartLastSecond = _.cloneDeep(this.optionChartOverall);
    this.optionChartLastSecond.title.text = 'Traffic distribution per replica (last second)';

    this.optionChartLastSecond_2 = _.cloneDeep(this.optionChartOverall);


    this.dataOverall = {
      labels: [],
      datasets: [{
          label: "Percentage",
          data: [],
          backgroundColor: '#1879c933',
          borderColor: '#1879c9',
          borderWidth: 1,
          maxBarThickness: 150, 
          hoverBackgroundColor:'#1879c966'
      }]
    }

    this.dataOverall_2 = _.cloneDeep(this.dataOverall);
    this.dataOverall_2.datasets[0].backgroundColor = '#FA8D1233';
    this.dataOverall_2.datasets[0].borderColor = '#FA8D12';
    this.dataOverall_2.datasets[0].hoverBackgroundColor = '#FA8D1266';

    this.dataLastSecond = _.cloneDeep(this.dataOverall); //deep copy of the object dataOverall

    this.dataLastSecond_2 = _.cloneDeep(this.dataOverall_2);
  
  }
  

  readData(){
    if(this.props.record.configuration !== "PF_WMR"){
      var replicas = this.props.record.replicas;
      
      parseReplicasOverall(this.dataOverall,replicas,undefined);
      parseReplicasLastSecond(this.dataLastSecond,this.state.previousDataLastSecond,replicas,undefined)

    }
    else {
      var replicas_1 = this.props.record.replicas_1;
      var replicas_2 = this.props.record.replicas_2;
      var name_stage_1 = this.props.record.name_stage_1;
      var name_stage_2 = this.props.record.name_stage_2;
      
      this.optionChartOverall.title.text = 'Traffic distribution per ' + name_stage_1+ ' replica (overall elapsed time)';
      this.optionChartOverall_2.title.text = 'Traffic distribution per ' + name_stage_2+ ' replica (overall elapsed time)';
      
      this.optionChartLastSecond.title.text = 'Traffic distribution per ' + name_stage_1+ ' replica (last second)';
      this.optionChartLastSecond_2.title.text = 'Traffic distribution per ' + name_stage_2+ ' replica (last second)';
      
      parseReplicasOverall(this.dataOverall,replicas_1,name_stage_1);
      parseReplicasOverall(this.dataOverall_2,replicas_2,name_stage_2);

      parseReplicasLastSecond(this.dataLastSecond,this.state.previousDataLastSecond,replicas_1,name_stage_1);
      parseReplicasLastSecond(this.dataLastSecond_2,this.state.previousDataLastSecond_2,replicas_2,name_stage_2);

    }
  }
  
  render(){
    this.readData();

    var chartOverall_1 = <Bar data={this.dataOverall} options={this.optionChartOverall} />;
    var chartLastSecond_1 = <Bar data={this.dataLastSecond} options={this.optionChartLastSecond}/>;
    var chartOverall_2 = undefined;
    var chartLastSecond_2 = undefined;

    if(this.props.record.configuration === "PF_WMR"){ 
      chartOverall_2 = <Bar data={this.dataOverall_2} options={this.optionChartOverall_2} />;
      chartLastSecond_2 = <Bar data={this.dataLastSecond_2} options={this.optionChartLastSecond_2}/>
    }

    return (
      <>
        <h2 className="title-statistic">Traffic distribution</h2> {/*TODO cambiare colore*/}
        <GridTrafficDistribution 
          chartOverall_1={ chartOverall_1 } 
          chartOverall_2={ chartOverall_2 } 
          chartLastSecond_1={chartLastSecond_1} 
          chartLastSecond_2={chartLastSecond_2}
        />
      </>
    )
  }
};



function parseReplicasOverall(data,replicas,name_stage){
  var totalInputReceived=0;
  data.datasets[0].data = []
  data.labels = []

  for(var i in replicas){
    totalInputReceived = totalInputReceived+ replicas[i].Inputs_received;
  }

  for(i in replicas){
    data.labels.push(name_stage ? replicas[i].Replica_id+" - "+name_stage : replicas[i].Replica_id);
    var percentageReplica = Number.parseFloat((replicas[i].Inputs_received/totalInputReceived)*100).toFixed(2);
    data.datasets[0].data.push(percentageReplica)
  }
}


function parseReplicasLastSecond(data,previous_data,replicas,name_stage){
  var overallInputReceived=0;
  
  for(var i in replicas){
    overallInputReceived = overallInputReceived+ replicas[i].Inputs_received;
  }


  var actualInputReceived = overallInputReceived - previous_data.overall_input_received;
  
  if(actualInputReceived === 0) {
    return
  }

  previous_data.overall_input_received = overallInputReceived;

  data.datasets[0].data = []
  data.labels = []

  for(i in replicas){
    data.labels.push(name_stage ? replicas[i].Replica_id+" - "+name_stage : replicas[i].Replica_id);
    
    if(previous_data.replica_input_received[i] === undefined) var actualInputReceived = replicas[i].Inputs_received;
    else var actualReplicaInputReceived = replicas[i].Inputs_received - previous_data.replica_input_received[i];
    previous_data.replica_input_received[i] = replicas[i].Inputs_received;
    
    var percentageReplica = Number.parseFloat((actualReplicaInputReceived/actualInputReceived)*100).toFixed(2);
    
    data.datasets[0].data.push(percentageReplica)
  }

  // return data
}