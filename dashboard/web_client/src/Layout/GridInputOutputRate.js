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

import React from "react";
import { Row, Col } from 'antd';


export default function GridInputOutputRate(props){
  var span = !props.chartInput_2 ? 24 : 12;
  var margin_left = 20;
  var margin_right = 20;

  return(
    <>
      <Row gutter={[0, 24]} style={{marginLeft:margin_left, marginRight:margin_right}}>
        <Col span={span} style={{height:250}} >
          {props.chartInput_1}
        </Col>
        { props.chartInput_2 ?
          <Col span={span} style={{height:250}} offset={0}>
            {props.chartInput_2}
          </Col> :
          undefined
        }
      </Row>
      <Row style={{marginLeft:margin_left, marginRight:margin_right}}>
        <Col span={span} style={{height:250}}>
          {props.chartOutput_1}
        </Col>
        { props.chartOutput_2 ?
          <Col span={span} style={{height:250}} offset={0}>
            {props.chartOutput_2}
          </Col> :
          undefined
        }
      </Row>
    </>
  )
}