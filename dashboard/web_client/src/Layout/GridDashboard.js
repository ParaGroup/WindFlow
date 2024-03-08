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
import { Row, Col } from 'antd';


export default function GridDashboard(props){
  return(
    <div className="grid-dashboard">
      <Row justify="space-around" gutter={[{ xs: 1, sm: 10, md: 13, lg: 100},100]} align="middle">
        <Col span={6}>
          {props.runningApp}
        </Col>
        <Col span={6}>
          {props.terminatedApp}
        </Col>
        <Col span={6}>
          {props.interruptedApp}
        </Col>
      </Row>

      <Row gutter={[0,100]}>
        <Col span={24}>
          {props.tableRunApplication}
        </Col>
      </Row>

      <Row>
        <Col span={24} style={{borderRadius:'200px'}}>
          {props.tableTermApplication}
        </Col>
      </Row>
    </div>
  );
  
}