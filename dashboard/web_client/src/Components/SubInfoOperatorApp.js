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
import { Card, Descriptions} from 'antd';

//TODO capire che è sto componente

export default function SubInfoOperatorApp(props){
  var windowParameters = props.windowParameters
  var body = isWindow(windowParameters);


  return (
    <Card style={{backgroundColor:'white', marginLeft:'50px'}}>
        {body}
    </Card>
  )
}


function isWindow(windowParameters){
  var items = (
    <div style={{width:'100%'}}>
    <Descriptions title="Window parameter" style={{backgroundColor:'white'}} extra="prova" column={4}>
        <Descriptions.Item label="Type">{windowParameters.window_type}</Descriptions.Item>
        {windowParameters.window_type === 'time-based'?<Descriptions.Item label="Delay">{windowParameters.lateness}</Descriptions.Item> : undefined}
        <Descriptions.Item label="Length">{windowParameters.window_length}</Descriptions.Item>
        <Descriptions.Item label="Slide">{windowParameters.window_slide}</Descriptions.Item>
    </Descriptions>
    </div>
  )

  return items
}