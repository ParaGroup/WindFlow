/*******************************************************************************
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
 *  Functors (lift and combine) of the different associative and commutative
 *  aggregation functions.
 */ 

// includes
#include<cmath>
#include<string>
#include<fstream>

using namespace std;

// lift functor of the SUM aggregate
class Lift_SUM_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const input_t &tuple, output_v1_t &result)
    {
        result.key = tuple.key;
        result._1 = tuple._1;
    }
};

// combine functor of the SUM aggregate
class Combine_SUM_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const output_v1_t &input1, const output_v1_t &input2, output_v1_t &result)
    {
        result.key = input1.key;
        result._1 = input1._1 + input2._1;
    }
};

// lift functor of the COUNT aggregate
class Lift_COUNT_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const input_t &tuple, output_v1_t &result)
    {
        result.key = tuple.key;
        result._1 = 1;
    }
};

// combine functor of the COUNT aggregate
class Combine_COUNT_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const output_v1_t &input1, const output_v1_t &input2, output_v1_t &result)
    {
        result.key = input1.key;
        result._1 = input1._1 + input2._1;
    }
};

// lift functor of the MAX aggregate
class Lift_MAX_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const input_t &tuple, output_v1_t &result)
    {
        result.key = tuple.key;
        result._1 = tuple._1;
    }
};

// combine functor of the MAX aggregate
class Combine_MAX_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const output_v1_t &input1, const output_v1_t &input2, output_v1_t &result)
    {
        result.key = input1.key;
        result._1 = (input1._1 > input2._1) ? input1._1 : input2._1;
    }
};

// lift functor of the MAX_COUNT aggregate
class Lift_MAX_COUNT_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const input_t &tuple, output_v2_t &result)
    {
        result.key = tuple.key;
        result._1 = 1;
        result._2 = tuple._2;
    }
};

// combine functor of the MAX_COUNT aggregate
class Combine_MAX_COUNT_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const output_v2_t &input1, const output_v2_t &input2, output_v2_t &result)
    {
        result.key = input1.key;
        if (input1._2 > input2._2) {
            result._2 = input1._2;
            result._1 = input1._1;
        }
        else if (input1._2 < input2._2) {
            result._2 = input2._2;
            result._1 = input2._1;
        }
        else {
            result._2 = input2._2;
            result._1 = input1._1 + input2._1;
        }
    }
};

// lift functor of the MIN aggregate
class Lift_MIN_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const input_t &tuple, output_v1_t &result)
    {
        result.key = tuple.key;
        result._1 = tuple._1;
    }
};

// combine functor of the MIN aggregate
class Combine_MIN_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const output_v1_t &input1, const output_v1_t &input2, output_v1_t &result)
    {
        result.key = input1.key;
        result._1 = (input1._1 < input2._1) ? input1._1 : input2._1;
    }
};

// lift functor of the MIN_COUNT aggregate
class Lift_MIN_COUNT_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const input_t &tuple, output_v2_t &result)
    {
        result.key = tuple.key;
        result._1 = 1;
        result._2 = tuple._2;
    }
};

// combine functor of the MIB_COUNT aggregate
class Combine_MIN_COUNT_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const output_v2_t &input1, const output_v2_t &input2, output_v2_t &result)
    {
        result.key = input1.key;
        if (input1._2 < input2._2) {
            result._2 = input1._2;
            result._1 = input1._1;
        }
        else if (input1._2 > input2._2) {
            result._2 = input2._2;
            result._1 = input2._1;
        }
        else {
            result._2 = input2._2;
            result._1 = input1._1 + input2._1;
        }
    }
};

// lift functor of the AVG aggregate
class Lift_AVG_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const input_t &tuple, output_v2_t &result)
    {
        result.key = tuple.key;
        result._1 = 1;
        result._2 = tuple._2;
    }
};

// combine functor of the AVG aggregate
class Combine_AVG_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const output_v2_t &input1, const output_v2_t &input2, output_v2_t &result)
    {
        result.key = input1.key;
        float alpha1 = (((float) input1._1) / (input1._1 + input2._1));
        float alpha2 = (((float) input2._1) / (input1._1 + input2._1));
        result._2 =  alpha1 * input1._2 + alpha2 * input2._2;
        result._1 + input1._1 + input2._1;
    }
};

// lift functor of the GEOM aggregate
class Lift_GEOM_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const input_t &tuple, output_v2_t &result)
    {
        result.key = tuple.key;
        result._1 = 1;
        result._2 = tuple._2;
    }
};

// combine functor of the GEOM aggregate
class Combine_GEOM_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const output_v2_t &input1, const output_v2_t &input2, output_v2_t &result)
    {
        result.key = input1.key;
        float r1 = pow(input1._2, input1._1);
        float r2 = pow(input2._2, input2._1);
        result._1 = input1._1 + input2._1;
        result._2 = pow((r1 * r2), result._1);
    }
};

// lift functor of the SSTD aggregate
class Lift_SSTD_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const input_t &tuple, output_v3_t &result)
    {
        result.key = tuple.key;
        result._1 = 1;
        result._2 = tuple._2;
        result._3 = pow(tuple._2, 2);
        result._4 = sqrt((1.0/((float) result._1)) * (result._3 - pow(result._2, 2)/result._1));
    }
};

// combine functor of the SSTD aggregate
class Combine_SSTD_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const output_v3_t &input1, const output_v3_t &input2, output_v3_t &result)
    {
        result.key = input1.key;
        result._1 = input1._1 + input2._1;
        result._2 = input1._2 + input2._2;
        result._3 = input1._3 + input2._3;
        result._4 = sqrt((1.0/((float) (result._1 - 1))) * (result._3 - pow(result._2, 2)/result._1));
    }
};

// lift functor of the PSTD aggregate
class Lift_PSTD_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const input_t &tuple, output_v3_t &result)
    {
        result.key = tuple.key;
        result._1 = 1;
        result._2 = tuple._2;
        result._3 = pow(tuple._2, 2);
        result._4 = sqrt((1.0/((float) result._1)) * (result._3 - pow(result._2, 2)/result._1));
    }
};

// combine functor of the PSTD aggregate
class Combine_PSTD_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const output_v3_t &input1, const output_v3_t &input2, output_v3_t &result)
    {
        result.key = input1.key;
        result._1 = input1._1 + input2._1;
        result._2 = input1._2 + input2._2;
        result._3 = input1._3 + input2._3;
        result._4 = sqrt((1/((float) result._1)) * (result._3 - pow(result._2, 2)/result._1));
    }
};
