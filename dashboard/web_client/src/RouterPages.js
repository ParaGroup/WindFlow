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
import { BrowserRouter as Router, Switch, Route, Redirect } from 'react-router-dom';
import Layout from './Layout/Layout'
import Dashboard from './Pages/Dashboard'
import App from './Pages/App';
import NotFound from './Pages/404'
import SomethingWentWrong from './Pages/500'

export default class RouterPages extends React.Component{
  constructor(props){
    super(props);
    this.updateServer = this.updateServer.bind(this);
    this.pushAppSwitcher = this.pushAppSwitcher.bind(this);
    this.appSwitch=[];

    this.state = {
      serverConnection: false,
    }
  }
  
  updateServer(state){
    if(this.state.serverConnection !== state) this.setState({serverConnection:state});
  }
 
  pushAppSwitcher(app){
    for(var i in this.appSwitch){
      if(app.id === this.appSwitch[i].id){
        this.appSwitch.splice(i,1); //remove from array the app with the same id
        break
      }
    }

    !this.appSwitch ? this.appSwitch[0]=app : this.appSwitch.splice(0,0,app);
    if(this.appSwitch.length > 5) this.appSwitch.pop()
  }

  render(){
    return(
      <Router>
        <Layout serverStatus={this.state.serverConnection} appSwitch={this.appSwitch} pushAppSwitcher={this.pushAppSwitcher}>
          <Switch>
            <Route exact path="/" render={() => <Redirect to="/dashboard" />} />
            <Route path='/dashboard' render={() => <Dashboard updateServerConnection={this.updateServer} serverConnection={this.state.serverConnection} pushAppSwitcher={this.pushAppSwitcher}/>} />
            <Route path='/app/:id' render={(props) => <App updateServerConnection={this.updateServer} serverConnection={this.state.serverConnection} {...props}/>} />
            <Route  path='/500' component={SomethingWentWrong} /> {/*TODO forse eliminare */}
            <Route component={NotFound} />
          </Switch>
        </Layout>

      </Router>
    )
  }
}