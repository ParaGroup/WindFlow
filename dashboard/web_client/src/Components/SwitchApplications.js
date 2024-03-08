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

import React, { useState } from 'react'
import { Drawer, Tooltip ,Tag } from 'antd';
import { SwapOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';

export default function SwitchApplication(props){

    const [visible, setVisible] = useState(false);
    const showDrawer = () => {
      setVisible(true);
    };
    const onClose = () => {
      setVisible(false);
    }

    var arrayAppSwitch = props.appSwitch;

    return (
      <>
        <Tooltip placement="bottomRight" title={"App switcher"}>
          <Tag  icon={<SwapOutlined style={{display: 'block', margin:'5px',}} />} color="orange" onClick={showDrawer} style={{top:4, position:'relative', padding:0, lineHeight:'normal', height:25, margin:0, borderRadius:50 }}/>
        </Tooltip>
        <Drawer
          title="LAST 5 APP SEEN"
          placement="right"
          closable={false}
          onClose={onClose}
          visible={visible}
        >
        <div>
        <div className="switch-application-legend">ID&nbsp;&nbsp;&nbsp;APP NAME</div>
          {
            arrayAppSwitch.map(item =>{
              return (
                <Link key={item.id} to={`/app/${item.id}`}>
                    <p onClick={onClose} className="switch-application-row">&#8226;&nbsp;&nbsp;{item.id}&nbsp;&nbsp;{item.name}</p>
                </Link>
              )
            })
          }
        </div>
        </Drawer>
      </>
    );
}