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
import { Table, Tag, Card, Empty } from 'antd';
import { LoadingOutlined, SyncOutlined, CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import '../General.css'
import SubTabPaneApp from './SubTabPaneApp';
import WFlogo from '../Icon/WindFlow_gray_logo.svg'
import TagConfigurationTableApp from './TagConfigurationTableApp'

export default function TableApp(props) {
  const columns = [
    { title: <b className='titleColumnTable'>Operator name</b>, dataIndex: 'operator_name', key: 'operator_name', width: 100, align: 'center',},
    { title: <b className='titleColumnTable'>Operator type</b>, dataIndex: 'operator_type', key: 'operator_type', width: 100, align: 'center' },
    { title: <b className='titleColumnTable'>Configuration</b>, dataIndex: 'configuration', key: 'configuration', width: 115, align: 'center', render: (text,record) => TagConfigurationTableApp(text,record) },
    { title: <b className='titleColumnTable'>Distribution</b>, dataIndex: 'distribution', key: 'distribution', width: 100, align: 'center' },
    { title: <b className='titleColumnTable'>Parallelism</b>, dataIndex: 'parallelism', key: 'parallelism', width: 100, align: 'center' },
    { title: <b className='titleColumnTable'>Status</b>, dataIndex: 'status', key: 'status',  width: 100,
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
  ];


  const readOperators = (props) => {
    var data = [];
    var operators = props.operators;
    
    if(operators === undefined) return data;

    for(var i in operators){
      var status = props.interrupted ? 'INTERRUPTED' : operators[i].isTerminated ? 'TERMINATED' : 'RUNNING' //check application status

      if(operators[i].Operator_type.includes("Paned_Windows") || operators[i].Operator_type.includes("MapReduce_Windows")){
        data.push(parse_PF_WMR_Operator(operators[i],i,status));
      }
      else{
        data.push(parseOperator(operators[i],i,status));
      }
      
    }
    return data
  }

  return (
    <Table
      columns={columns}
      dataSource={readOperators(props)}
      rowClassName="row-table-app"
      expandable={{
        expandRowByClick: () => true,
        expandedRowRender: (record,index,indent,expanded) =>{

            if(expanded){
              if((record.variable_parameters).areNestedOPs){
                return (
                  <Card
                    style={{
                      boxShadow: '0 1px 2px 0 rgba(0, 0, 0, 0.2), 0 3px 10px 0 rgba(0, 0, 0, 0.19)', 
                      borderRadius:'10px',
                      paddingTop:'25px',
                      paddingBottom:'25px',
                    }}
                  >
                    <TableApp operators={record.replicas} interrupted={props.interrupted} style={{marginLeft:-25}} idApp={props.idApp} idOperator={index}/>
                  </Card>
                )
              }
              else{
                var idOperator = !props.idOperator ? index : props.idOperator+"_"+index;
                return <SubTabPaneApp record={record} interrupted={props.interrupted} idApp={props.idApp} idOperator={idOperator}/>
              }
            }
            return []
        },
        }}
      style={props.style}
      tableLayout="fixed" 
      pagination={false}
      loading={
        (props.operators === undefined) ? { indicator: <LoadingOutlined style={{ fontSize: 40 }} spin /> } : false
      }
      scroll={{x:1000}}
      locale={{
          emptyText:
            <Empty 
              image={WFlogo}
              imageStyle={{
                height:50,
                marginLeft:20
              }}
              description={'Fetching data'}
            />
      }}
    />
  );  
}


// function that parse normal and nested operators
function parseOperator(operator, key, status){
  var object = {
    key: key,
    operator_name: operator.Operator_name,
    operator_type: operator.Operator_type,
    distribution: operator.Distribution,
    parallelism: operator.Parallelism,
    historical_data: operator.Historical_data,
    isWindowed: operator.isWindowed,
    isGPU: operator.isGPU,
    replicas: operator.Replicas,
    status: [status],
    configuration: operator.isWindowed ? "Windowed" : "Basic", //parameter used for cpu and window tag
    
    variable_parameters:{
      // window parameters
      window_type: operator.Window_type,
      lateness: operator.Lateness,
      window_length: operator.Window_length,
      window_slide: operator.Window_slide,

      // gpu parameters
      batch_len: operator.Batch_len,

      // nested operators
      areNestedOPs: operator.areNestedOPs,
    }
  }
  
  return object;
}

// Function that parse only PF e WMR operators
function parse_PF_WMR_Operator(operator, key, status){
  var object = {
    key: key,
    operator_name: operator.Operator_name,
    operator_type: operator.Operator_type,
    distribution: operator.Distribution,
    parallelism: operator.Parallelism_1+operator.Parallelism_2,
    isGPU_1: operator.isGPU_1,
    isGPU_2: operator.isGPU_2,
    name_stage_1: operator.Name_Stage_1,
    name_stage_2: operator.Name_Stage_2,
    replicas_1: operator.Replicas_1,
    replicas_2: operator.Replicas_2,
    status: [status],
    configuration: "PF_WMR",

    variable_parameters:{
      // window parameters
      window_1: {
        window_type:operator.Window_type_1,
        lateness: operator.Lateness,
        window_length: operator.Window_length_1,
        window_slide: operator.Window_slide_1,
      },

      window_2: {
        window_type: operator.Window_type_2,
        window_length: operator.Window_length_2,
        window_slide: operator.Window_slide_2,
      },

      
      // gpu parameters
      batch_len: operator.Batch_len
    }

  }

  return object;
}


