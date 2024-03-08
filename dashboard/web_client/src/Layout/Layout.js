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
import { Layout, Divider } from 'antd';
import HeaderButton from '../Components/HeaderButton'
import ServerStatusHeader from '../Components/ServerStatusHeader'
import SwitchApplication from '../Components/SwitchApplications'
import {ReactComponent as Unipi_logo} from '../Icon/unipi_logo.svg'


const { Header, Content, Footer } = Layout;

class LayoutPage extends React.Component {
  render() {
    return (
      <Layout className="layout" style={{ minHeight:'100vh'}} hasSider={false}>
        <Header style={{position: 'fixed', width:'100%', zIndex: 1, backgroundColor:'#05335F', paddingLeft:'20px', paddingTop: '6px'}}>
            <div className='right-elements-header'>
              <ServerStatusHeader serverStatus={this.props.serverStatus} />
              <Divider type="vertical" style={{backgroundColor:"white", top:10}}/>
              <SwitchApplication appSwitch={this.props.appSwitch} pushAppSwitcher={this.props.pushAppSwitcher}/>
            </div>
            <HeaderButton />
        </Header>
        <Content 
          style={{ padding: '55px', minHeight: 280, marginTop:64 }}
        >
          {this.props.children}
        </Content>
        <Footer style={{ textAlign: 'center', backgroundColor:'#05335F', padding:'0px'}}><Unipi_logo /></Footer>
      </Layout>
    );
  }
}

export default LayoutPage;