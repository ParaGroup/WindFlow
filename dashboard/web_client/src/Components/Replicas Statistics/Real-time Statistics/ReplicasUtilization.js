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
import { Progress, List } from 'antd';


export default function ReplicasUtilization(props){
  var data = [];
  var margin;
  
  if(props.record.configuration !== "PF_WMR"){
    var replicas = props.record.replicas;
    data = parseReplicas(data,replicas, undefined);
    margin=55
  }
  else{
    var replicas_1 = props.record.replicas_1;
    var replicas_2 = props.record.replicas_2;
    
    data = parseReplicas(data,replicas_1,props.record.name_stage_1)
    data = parseReplicas(data,replicas_2,props.record.name_stage_2)

    margin=30;
  }
  
  
  return (
    <>
      <h2 className="title-statistic">Average replicas utilization</h2> {/*TODO cambiare colore*/}
      <List 
        grid={{ gutter: 16, column: 5 }}
        dataSource={data}
        renderItem={item => {
          var strokeColor = '#1890ff'
          
          if(item.usage >=33 && item.usage < 66) strokeColor = '#F39E23';
          else if(item.usage > 66) strokeColor = '#F32222';
          
          return(
            <List.Item>
              <div>
              <Progress percent={item.usage} type="dashboard" strokeWidth={5} strokeColor={strokeColor}/>
              <div style={{marginLeft:margin, fontWeight:'bold'}}>{item.id}</div>
              </div>
            </List.Item>
          )
        }}
        style={{marginRight:24}}
      />
    </>
  )
}

function parseReplicas(data,replicas,name_stage){

  for(var i in replicas){
    var object = {
      id: name_stage ? replicas[i].Replica_id+" - "+name_stage : replicas[i].Replica_id,
      usage: Number.parseFloat((replicas[i].Service_time_usec/replicas[i].Eff_Service_time_usec)*100).toFixed(2),
    }
    data.push(object);
  }

  return data;
}