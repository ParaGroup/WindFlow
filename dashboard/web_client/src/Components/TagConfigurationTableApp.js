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
import { Tag, Popover, Space, Badge } from 'antd';
import Icon from '@ant-design/icons';
import { ApartmentOutlined } from '@ant-design/icons';
import {ReactComponent as VideoCardSVG} from '../Icon/video-card.svg';
import {ReactComponent as WindowSVG} from '../Icon/window.svg'


const VideoCardIcon = props => <Icon component={VideoCardSVG} {...props} />
const WindowIcon = props => <Icon component={WindowSVG} {...props} />

// Function that create tag for the configuration of the operator
export default function TagConfigurationTableApp(text,record){
  if(text === "PF_WMR") {
    var isGPU_1 = record.isGPU_1;
    var isGPU_2 = record.isGPU_2
    var variable_parameters = record.variable_parameters;
    var contentGPU;
    var contentWindow;

    if(isGPU_1 || isGPU_2){
      contentGPU = (
        <ul style={{ paddingInlineStart:15 }}>
          <li>
            <div className="labelPopover">GPU operator:</div> {isGPU_1 ? record.name_stage_1 : record.name_stage_2}
              {gpuPopoverContent(variable_parameters)}
          </li>
        </ul>
      )
    }

    contentWindow = (
      <ul style={{ paddingInlineStart:15 }}>
        <li>
          <div className="labelPopover">Name stage:</div> {record.name_stage_1}
            {windowPopoverContent(variable_parameters.window_1)}
        </li>
        <br/>
        <li>
          <div className="labelPopover">Name stage:</div> {record.name_stage_2}
            {windowPopoverContent(variable_parameters.window_2)}
        </li>
      </ul>
    )

    return tag(isGPU_1||isGPU_2,true,contentGPU,contentWindow);
  }
  else if(text === "Windowed"){
    var variable_parameters = record.variable_parameters;
    var isWindowed = record.isWindowed;
    var isGPU = record.isGPU;

    return tag(isGPU,isWindowed,gpuPopoverContent(variable_parameters),windowPopoverContent(variable_parameters))
  }

  if (record.isGPU) {
  	return <Tag icon={<ApartmentOutlined />} color="default" style={{fontSize:15}}>{"BASIC GPU"}</Tag>
  }
  else {
  	return <Tag icon={<ApartmentOutlined />} color="default" style={{fontSize:15}}>{"BASIC CPU"}</Tag>
  }
  
}



function tag(isGPU,isWindowed,contentGPU,contentWindow){
  var titlePopover = (text) => <b className="titlePopover">{text}</b>

  var status = "warning";
  var offset = [-8,0];
  
  return (
      <Space size={5}direction="horizontal" >
      { isGPU ?
          <Popover content={contentGPU} title={titlePopover("GPU parameters")} placement="topLeft">
            <Badge status={status} offset={offset}>
              <Tag icon={<VideoCardIcon />} color="success" style={{fontSize:15}} >GPU</Tag>
            </Badge>
          </Popover>
          : undefined
        }

        { isWindowed ? 
          <Popover content={contentWindow} title={titlePopover("Window Parameters")}  placement="topLeft">
            <Badge status={status} offset={offset}>
              <Tag icon={<WindowIcon />} color="blue" style={{fontSize:15}}>{isGPU ? "WINDOWED_GPU" : "WINDOWED_CPU"}</Tag>
            </Badge>
          </Popover>
          : undefined
        }
      </Space>
  )
}

 //function for manipulate window parameters of the operator
function windowPopoverContent (variable_parameters) {
  var type = variable_parameters.window_type;
  var delay = variable_parameters.lateness;
  var length = variable_parameters.window_length;
  var slide = variable_parameters.window_slide;
  
  var timeConverter = time =>{
    var millisecond = 1000;
    var second = millisecond * 1000;

    if((time >= 0) && (time < millisecond)){
      return time+" μs";
    }
    else if((time >= millisecond) && (time < second)){
      return (time / millisecond)+" ms"
    }
    else{
      return (time / second)+" s"
    }
  }

  if(type === "time-based"){
    delay = timeConverter(delay);
    length = timeConverter(length);
    slide = timeConverter(slide);
  }

  return(
    <ul style={{ paddingInlineStart:15 }}>
      <li><div className="labelPopover">Type:</div> {type}</li>
      {type === 'time-based'? <li><div className="labelPopover">Lateness:</div> {delay}</li> : undefined}
      <li><div className="labelPopover">Length:</div> {length}</li>
      <li><div className="labelPopover">Slide:</div> {slide}</li>
    </ul>
  )
}

//function for manipulate gpu parameters of the operator
function gpuPopoverContent(variable_parameters) {
  var batch_len = variable_parameters.batch_len

  return(
    <ul style={{ paddingInlineStart:15 }}>
      <li><div className="labelPopover">Batch len:</div> { batch_len }</li>
    </ul>
  )
}