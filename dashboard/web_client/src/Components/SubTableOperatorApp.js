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
import { SyncOutlined, CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import { Table, Tag, } from 'antd';


export default function SubTableOperatorApp(props){
  var isWindowed = props.record.isWindowed;
  var isGPU = props.record.isGPU ? props.record.isGPU : (props.record.isGPU_1 || props.record.isGPU_2); //check if the normal operator or PF/WMR is on GPU
  var configuration = props.record.configuration;
  var isInterrupted = props.interrupted
  
  const columns = [
    { title: <b className='titleColumnTable'>Replica ID</b>, dataIndex: 'replica_id', key: 'replica_id', width: 100, fixed:'left', align: 'center', },
    { title: <b className='titleColumnTable'>Start time</b>, dataIndex: 'start_time', key: 'start_time', width: 200, align: 'center', },
    { title: <b className='titleColumnTable'>Running time</b>, dataIndex: 'running_time', key: 'running_time', width: 100, align: 'center', },
    { title: <b className='titleColumnTable'>Input received</b>, dataIndex: 'input_received', key: 'input_received', width: 130, align: 'center', },
    { title: <b className='titleColumnTable'>Bytes received</b>, dataIndex: 'bytes_received', key: 'bytes_received', width: 130, align: 'center', },
    { title: <b className='titleColumnTable'>Output sent</b>, dataIndex: 'output_sent', key: 'output_sent', width: 130, align: 'center', },
    { title: <b className='titleColumnTable'>Bytes sent</b>, dataIndex: 'bytes_sent', key: 'bytes_sent', width: 130, align: 'center', },
    { title: <b className='titleColumnTable'>Service time<br />(avg)</b>, dataIndex: 'service_time', key: 'service_time', width: 110, align: 'center', render: text => timeConverter(text) },
    { title: <b className='titleColumnTable'>Eff service time<br />(avg)</b>, dataIndex: 'eff_service_time', key: 'eff_service_time', width: 120, align: 'center', render: text => timeConverter(text) },
  ];


  if(isWindowed || configuration === "PF_WMR"){
    var variableFields = {title: <b className='titleColumnTable'>Inputs ignored</b>, dataIndex: 'inputs_ignored', key: 'inputs_ignored', width: 130, align: 'center' };
    columns.splice(4,0,variableFields)
  }
  

  // if(isGPU){
  //   var variableFields = [
  //     { title: <b className='titleColumnTable'>Kernels launched</b>, dataIndex: 'kernels_launched', key: 'kernels_launched', width: 100, align: 'center', },
  //     { title: <b className='titleColumnTable'>Bytes H2D</b>, dataIndex: 'bytes_H2D', key: 'bytes_H2D', width: 110, align: 'center', },
  //     { title: <b className='titleColumnTable'>Bytes D2H</b>, dataIndex: 'bytes_D2H', key: 'bytes_D2H', width: 110, align: 'center', },
  //   ];

  //   for(var i in variableFields){
  //     columns.push(variableFields[i]);
  //   }
  // }

  if(configuration === "PF_WMR"){
    var variableFields = {title: <b className='titleColumnTable'>Name stage</b>, dataIndex: 'name_stage', key: 'name_stage', width: 100, align: 'center', fixed: 'right'};
    columns.push(variableFields);
  }

  { //pushing status column
    var variableFields = { title: <b className='titleColumnTable'>Status</b>, dataIndex: 'status', key: 'status',  width: 140, fixed: 'right',
      render: tags => (
        <>
          {tags.map(tag => {
            let color = 'geekblue';
            let icon = <SyncOutlined spin/>

            if (tag === 'TERMINATED') {
              color = 'green';
              icon = <CheckCircleOutlined />
            }
            else if (tag === 'INTERRUPTED'){
              color = 'volcano'
              icon = <CloseCircleOutlined />
            }

            return (
              <Tag color={color} key={tag} icon={icon} style={{borderRadius:'20px'}}>
                {tag.toUpperCase()}
              </Tag>
            );
          })}
        </>
      ),
    }
    columns.push(variableFields)
}


  var data = [];

  if(props.record.configuration !== "PF_WMR"){ //if the operator is PF o WMR
    var replicas = props.record.replicas;
    if(replicas === undefined) return data;

    data = parseReplicas(data,replicas,undefined,0,isInterrupted);
  }
  else{
    var replicas_1 = props.record.replicas_1;
    var replicas_2 = props.record.replicas_2;
    if(replicas_1 === undefined) return data;

    data = parseReplicas(data,replicas_1,props.record.name_stage_1,0,isInterrupted);    
    data = parseReplicas(data,replicas_2,props.record.name_stage_2,replicas_1.length,isInterrupted);
  }

  return (
    <Table 
      columns={columns}
      dataSource={data}
      pagination={false}
      tableLayout="fixed" 
      scroll={{x:1000}}
      style={{paddingRight:'50px'}}
    />
  );
}

function parseReplicas(data,replicas,name_stage,offset_key,isInterrupted){
  for (var i in replicas) {
    var status = isInterrupted ? 'INTERRUPTED' : replicas[i].isTerminated ? 'TERMINATED' : 'RUNNING' //check replica status

    data.push({
      key: i+offset_key,
      replica_id: replicas[i].Replica_id,
      start_time: replicas[i].Starting_time,
      running_time: secondToTime(replicas[i].Running_time_sec),
      input_received: replicas[i].Inputs_received,
      bytes_received: bytesToSize(replicas[i].Bytes_received,2),
      output_sent: replicas[i].Outputs_sent,
      bytes_sent: bytesToSize(replicas[i].Bytes_sent,2),
      service_time: Number.parseFloat(replicas[i].Service_time_usec).toFixed(4),
      eff_service_time: Number.parseFloat(replicas[i].Eff_Service_time_usec).toFixed(4),
      status: [status],

      // Additional parameters

      //isWindowed
      inputs_ignored: replicas[i].Inputs_ingored,

      //isGPU
      kernels_launched: replicas[i].Kernels_launched ? replicas[i].Kernels_launched : '-', // - is showed only for PF and WMR operators
      bytes_H2D: replicas[i].Bytes_H2D ? bytesToSize(replicas[i].Bytes_H2D,2) : '-', // - is showed only for PF and WMR operators
      bytes_D2H: replicas[i].Bytes_D2H ? bytesToSize(replicas[i].Bytes_D2H,2) : '-',

      // PF_WMR configuration
      name_stage: name_stage

    });
  }

  return data;
}


function timeConverter(time){
  var millisecond = 1000;
  var second = millisecond * 1000;

  if((time >= 0) && (time < millisecond)){
    return time+" μs";
  }
  else if((time >= millisecond) && (time < second)){
    return Number.parseFloat(time / millisecond).toFixed(4)+" ms"
  }
  else{
    return Number.parseFloat(time / second).toFixed(4)+" s"

  }
}

function secondToTime(s) {
  return new Date(s * 1000).toISOString().substr(11, 8);
}


function bytesToSize(bytes, precision){  
    var kilobyte = 1024;
    var megabyte = kilobyte * 1024;
    var gigabyte = megabyte * 1024;
    var terabyte = gigabyte * 1024;

    if ((bytes >= 0) && (bytes < kilobyte)) {
        return bytes + ' B';
 
    } else if ((bytes >= kilobyte) && (bytes < megabyte)) {
        return (bytes / kilobyte).toFixed(precision) + ' KB';
 
    } else if ((bytes >= megabyte) && (bytes < gigabyte)) {
        return (bytes / megabyte).toFixed(precision) + ' MB';
 
    } else if ((bytes >= gigabyte) && (bytes < terabyte)) {
        return (bytes / gigabyte).toFixed(precision) + ' GB';
 
    } else if (bytes >= terabyte) {
        return (bytes / terabyte).toFixed(precision) + ' TB';
 
    } else {
        return bytes + ' B';
    }
}