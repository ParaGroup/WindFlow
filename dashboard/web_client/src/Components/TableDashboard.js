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
import { Redirect } from 'react-router-dom';
import { Table, Tag, Empty } from 'antd';
import {
  CheckCircleOutlined,
  SyncOutlined,
  CloseCircleOutlined,
  LoadingOutlined,
  SearchOutlined
} from '@ant-design/icons';
import WFlogo from '../Icon/WindFlow_gray_logo.svg'
import _ from 'lodash'


const columnsRunning = [
  { title: '', width: 32, fixed: 'left', align: 'center', render: () => (<> <SearchOutlined/> </> ) },
  { title: <b className='titleColumnTable'>ID</b>, dataIndex: 'id', key: 'id', fixed: 'left', width: 70, align: 'center' },
  { title: <b className='titleColumnTable'>App name</b>, dataIndex: 'name', key: 'name', align: 'center' },
  { title: <b className='titleColumnTable'>Start time</b>, dataIndex: 'start', key: 'start', align: 'center', sortDirections: ['descend'], defaultSortOrder: 'descend', sorter: {compare: (a, b) => compareDate(a.start,b.start)}, },
  { title: <b className='titleColumnTable'>End time</b>, dataIndex: 'end', key: 'end', align: 'center', },
  { title: <b className='titleColumnTable'>Duration</b>, dataIndex: 'duration', key: 'duration', align: 'center', sorter: { compare: (a, b) => compareTime(a.duration,b.duration),}, },
  { title: <b className='titleColumnTable'>Remote IP</b>, dataIndex: 'remoteAddress', key: 'remoteAddress', align: 'center', },
  { title: <b className='titleColumnTable'>Status</b>, key: 'status', dataIndex: 'status', fixed: 'right', align: 'center',
    render: tags => (
      <>
        {tags.map(tag => {
          let color = 'geekblue';
          let icon = <SyncOutlined spin/>
          return (
            <Tag color={color} key={tag} icon={icon} style={{borderRadius:'20px'}}>
              {tag.toUpperCase()}
            </Tag>
          );
        })}
      </>
    ),
  },
];


const columnsTerminated = createColumnsTerminater(columnsRunning);


export default class TableDashboard extends React.Component{
  constructor(props){
    super(props);
    this.state = {
      idApp:-1,
      nameApp:undefined,
      redirect: false,
    }
  }

  setRedirect(id,name){ //used for go to App page
    this.setState({
      idApp: id,
      nameApp:name,
      redirect: true,
    }) 

    return 
  }

  render(){
    if(this.state.redirect){      
      this.props.pushAppSwitcher({name:this.state.nameApp, id:this.state.idApp})
      return(<Redirect push to={`/app/${this.state.idApp}`}/>)
    }
    
    var columns = this.props.table === 'terminated' ? columnsTerminated : columnsRunning; //check if show terminated o running columns

    return(
      <Table 
        rowKey={record =>{ return record.id}}
        rowClassName="row-table-dashboard"
        onRow={(record, rowIndex) => {
          return {
            onClick: event => {
              return this.setRedirect(record.id, record.name)
            },
          }
        }}
        columns={columns}
        dataSource={this.props.data} 
        locale={{
          emptyText:
            <Empty 
              image={WFlogo}
              imageStyle={{
                height:50,
                marginLeft:20
              }}
              description={'No '+this.props.table+' WindFlow application'}
            />
        }} 
        tableLayout="fixed" 
        scroll={{x:1000}}
        style={{boxShadow: '0 1px 2px 0 rgba(0, 0, 0, 0.2), 0 3px 10px 0 rgba(0, 0, 0, 0.19)', backgroundColor:'white', overflow:'auto'}}
        pagination={{style:{marginRight:'10px'}, hideOnSinglePage:true}}
        loading={
          !this.props.data ? ({ indicator: <LoadingOutlined style={{ fontSize: 40 }} spin /> }) : false
        }
      />
    ) 
  }
  
}


function createColumnsTerminater(columnsRunning){
  var cloneObj = _.cloneDeep(columnsRunning) //deep copy of the object columnsRunning
  
  var statusColumn = getColumn(cloneObj,"status");
  statusColumn.filters = [
    {
      text: 'Terminated',
      value: 'terminated',
    },
    {
      text: 'Interrupted',
      value: 'interrupted',
    },
  ];
  statusColumn.filterMultiple = false;
  statusColumn.onFilter = (value, record) => record.status == value;
  statusColumn.render= status => (
    <>
      {status.map(tag => {
        let color;
        let icon;
        if (tag === 'terminated') {
          color = 'green';
          icon = <CheckCircleOutlined />
        }
        else if (tag === 'interrupted'){
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
  )
  

  var endTimeColumn = getColumn(cloneObj,"end");

  endTimeColumn.sortDirections = ['descend'];
  endTimeColumn.sorter = {
    compare: (a, b) => compareDate(a.end,b.end),
  };

  return cloneObj;
}

function getColumn(jsonArray,column){
  for(var i in jsonArray){
    var item = jsonArray[i];
    if(item.key === column) return item
  }

  return undefined
}


function compareDate(a,b){
  var primo = new Date(a);
  var secondo = new Date(b)


  return primo - secondo
}

function compareTime(a,b){
  return (timeToSeconds(a)-timeToSeconds(b))
}

function timeToSeconds(time){
  var hms = time;
  var a = hms.split(':');

  var seconds = (+a[0]) * 60 * 60 + (+a[1]) * 60 + (+a[2]); 

  return seconds
}