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
import { Tabs } from 'antd';
import SubTableOperatorApp from './SubTableOperatorApp';
import ReplicasRealTimeStatisticsTabPane from './ReplicasRealTimeStatisticsTabPane'
import InputOutputRate from './Replicas Statistics/Historical Statistics/InputOutputRate'

const { TabPane } = Tabs;

export default function SubTabPaneApp(props){
  const component = (
    <div>
      <Tabs 
        defaultActiveKey="1" 
        centered 
        style={{
          boxShadow: '0 1px 2px 0 rgba(0, 0, 0, 0.2), 0 3px 10px 0 rgba(0, 0, 0, 0.19)', 
          borderRadius:'10px', 
          paddingBottom:'25px',
          backgroundColor: 'white'
        }}
        animated={true}
      >

        <TabPane tab="Replicas" key="1">
          <SubTableOperatorApp record={props.record} interrupted={props.interrupted}/>
        </TabPane>

        <TabPane tab="Real-time statistics" key="2">
          <ReplicasRealTimeStatisticsTabPane record={props.record} />
        </TabPane>

        <TabPane tab="Historical statistics" key="3">
          <InputOutputRate record={props.record} idApp={props.idApp} idOperator={props.idOperator}/>
        </TabPane>
      </Tabs>
    </div>
  );
  return (
    component
  );
}