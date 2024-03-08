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
import _ from 'lodash';
import conf from '../../../Configuration/config.json';
import {Line, Doughnut,Bar, Chart} from 'react-chartjs-2';
import GridInputOutputRate from '../../../Layout/GridInputOutputRate'
import { Spin } from 'antd';
import { LoadingOutlined } from '@ant-design/icons';


const antIcon = <LoadingOutlined style={{ fontSize: 45 }} spin />;


export default class InputOutputRate extends React.Component{
  constructor(props){
    super(props);
    this.state = {
      intervalID: undefined,
    }

    this.optionChartInput = {
      title: {
        display: true,
        text: 'Input rate',
        fontSize: 14
      },
      legend: {
        display: false,
      },
      maintainAspectRatio: false,
      elements: {
        line: {
            // tension: 0 // disables bezier curves
        }
      },
      scales: {
        yAxes: [{
          gridLines: {
            // drawOnChartArea: false,
          },
          ticks:{
            maxTicksLimit: 5
          },
          scaleLabel: {
            display: true,
            labelString: 'INPUTS/SECOND',
            fontSize: 11,
          }
        }],
        xAxes: [{
          gridLines: {
            drawOnChartArea: false,
          },
          ticks:{
            autoSkip: false,
            callback: function(tick, index, array) {
              if(index === array.length-1) return tick
              else if(array.length <=100) {
                if(index % 5) return undefined;
                else if(index >= array.length-3) return undefined;
                else return tick;
              }
              else if(array.length <=200) {
                if(index % 8) return undefined;
                else if(index >= array.length-4) return undefined;
                else return tick;
              }
              else return (index % 12) ? undefined : tick;
          }
          },
          scaleLabel: {
            display: true,
            labelString: 'ELAPSED SECONDS',
            fontSize: 11,
            padding: 8,
          },
          distribution: 'linear'
        }]
      },
      tooltips:{
        displayColors: false,
      }
    }

    this.optionChartInput_2 = _.cloneDeep(this.optionChartInput);

    this.optionChartOutput = _.cloneDeep(this.optionChartInput);
    this.optionChartOutput.title.text = 'Output rate'
    this.optionChartOutput.scales.yAxes[0].scaleLabel.labelString = "OUTPUTS/SECOND"

    this.optionChartOutput_2 = _.cloneDeep(this.optionChartOutput);

    this.inputData = {
      labels: [],
      datasets: [{
          data: [],
          backgroundColor: '#1879c933',
          borderColor: '#1879c9',
          pointBackgroundColor: '#1879c900',
          pointBorderColor: '#1879c900',
          borderCapStyle: 'round',
          borderWidth: 1
      }]
    }
    
    this.inputData_2 = _.cloneDeep(this.inputData);
    this.inputData_2.datasets[0].backgroundColor = '#FA8D1233';
    this.inputData_2.datasets[0].borderColor = '#FA8D12';
    this.inputData_2.datasets[0].pointBackgroundColor = '#FA8D1200';

    this.outputData = _.cloneDeep(this.inputData);
    this.outputData_2 = _.cloneDeep(this.inputData_2);
  }

  requestHistoricalData(){
    var xhr = new XMLHttpRequest();
    xhr.open("GET", `${conf.url}/get_historical_data?idApp=${this.props.idApp}&idOperator=${this.props.idOperator}`, true);
    xhr.onload = function (e) {

      if (xhr.readyState === 4) {
        if (xhr.status === 200) {
          
          var json_obj = JSON.parse(xhr.responseText);
          this.setData(json_obj);
          
        } 
        else {
          console.error(xhr.statusText);
        }
      }
    }.bind(this);

    xhr.onerror = function (e) {
      console.log(e)
    }.bind(this);

    xhr.send(null);
  }

  setData(json_obj){
    var record = this.props.record;


    this.inputData.labels = (json_obj.Historical_data).Inputs_received.Label
    this.inputData.datasets[0].data = (json_obj.Historical_data).Inputs_received.Inputs

    this.outputData.labels = (json_obj.Historical_data).Outputs_sent.Label
    this.outputData.datasets[0].data = (json_obj.Historical_data).Outputs_sent.Outputs

    // this.outputData.labels = (json_obj.full).Inputs_received.Label
    // this.outputData.datasets[0].data = (json_obj.full).Inputs_received.Inputs

    if(this.props.record.configuration === "PF_WMR"){
      this.inputData_2.labels = (json_obj.Historical_data_2).Inputs_received.Label
      this.inputData_2.datasets[0].data = (json_obj.Historical_data_2).Inputs_received.Inputs

      this.outputData_2.labels = (json_obj.Historical_data_2).Outputs_sent.Label
      this.outputData_2.datasets[0].data = (json_obj.Historical_data_2).Outputs_sent.Outputs

      var name_stage_1 = this.props.record.name_stage_1;
      var name_stage_2 = this.props.record.name_stage_2;

      this.optionChartInput.title.text = 'Input rate (' + name_stage_1 + ')';
      this.optionChartOutput.title.text = 'Output rate (' + name_stage_1+ ')'

      this.optionChartInput_2.title.text = 'Input rate (' + name_stage_2 + ')';
      this.optionChartOutput_2.title.text = 'Output rate (' + name_stage_2 + ')'
    }
  } 

  componentDidMount(){
    this.setState({intervalID:setInterval(this.requestHistoricalData.bind(this),conf.timeout)})
  }


  componentWillUnmount(){
    clearInterval(this.state.intervalID);
  }


  render(){
    var chartInput_1 = <Line data={this.inputData} options={this.optionChartInput}/>;
    var chartOutput_1 = <Line data={this.outputData} options={this.optionChartOutput}/>;
    var chartInput_2 = undefined;
    var chartOutput_2 = undefined;

    if(this.props.record.configuration === "PF_WMR"){ 
      chartInput_2 = <Line data={this.inputData_2} options={this.optionChartInput_2}/>;
      chartOutput_2 = <Line data={this.outputData_2} options={this.optionChartOutput_2}/>;
    }

    return(
      <>
        <h2 className="title-statistic" style={{marginLeft:35}}>Input & output rates</h2> {/*TODO cambiare colore*/}
        { this.inputData.labels.length === 0 ? 
          <Spin indicator={antIcon} style={{marginBottom:70, marginTop:70, marginLeft:'auto', marginRight:'auto',display:'block'}}/> :
          <GridInputOutputRate
            chartInput_1={chartInput_1}
            chartInput_2={chartInput_2}
            chartOutput_1={chartOutput_1}
            chartOutput_2={chartOutput_2}
          />
        }
      </>
    )
  }
}