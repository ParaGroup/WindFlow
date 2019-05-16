/******************************************************************************
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License version 3 as
 *  published by the Free Software Foundation.
 *  
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 *  License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 ******************************************************************************
 */

/*  
 *  Spatial Query Application
 *  
 *  Version where the Skyline operator is implemented by a Win_Farm pattern.
 */ 

// includes
#include <set>
#include <vector>
#include <string>
#include <iostream>
#include <algorithm>
#include <getopt.h>
#include <fstream>
#include <assert.h>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include "tuple_t.hpp"
#include "skytree.hpp"

using namespace std;

// main
int main(int argc, char *argv[])
{
	ifstream *file = new ifstream("data.txt");
	vector<tuple_t> inputSet;
	size_t id_tuple = 0;
	// read the file row by row
	string line;
	while(getline(*file, line)) {
   		stringstream s(line);
   		float dims[DIM];
   		int c=0;
   		while(!s.eof() && c<DIM) {
      		string tmp;
      		s >> tmp;
      		dims[c++] = stod(tmp);
   		}
   		tuple_t t;
   		t.key = 0;
   		t.id = id_tuple;
   		t.lid = id_tuple;
   		t.ts = 0;
   		for(size_t x=0; x<DIM; x++)
   			t.elems[x] = dims[x];
   		inputSet.push_back(t);
   		id_tuple++;
   	}
    cout << "inputSet size: " << inputSet.size() << endl;
    // compute the skyline
    SkyTree skytree(inputSet.size(), DIM, inputSet, true, false);
    skytree.Init();
    vector<int> skyIdx = skytree.Execute();
    // prepare the output set of skyline tuples
    vector<tuple_t> outputSet;
    for (auto &_t: inputSet) {
        if (find(skyIdx.begin(), skyIdx.end(), _t.lid) != skyIdx.end())
            outputSet.push_back(_t);
    }
	cout << "outputSet size: " << outputSet.size() << endl;
   	return 0;
}
