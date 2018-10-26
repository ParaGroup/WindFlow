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
 *  File implementing methods to generate pseudo-random numbers according to
 *  some distributions (e.g., uniform, exponential, normal).
 */ 

#ifndef RANDOM_H
#define RANDOM_H

// includes
#include <cmath>
#include <ctime>
#include <string>
#include <cstdlib>

using namespace std;

// exception class
class RandomGeneratorError
{
public:
	// constructor
	RandomGeneratorError(string _msg)
	{
		msg = "RandomGenerator Error: ";
		if (_msg.length() < 1)
			msg += "Undefined error.";
		else msg += _msg;
	}
	string msg;
};

// class RandomGenerator
class RandomGenerator
{
private:
	long seed;

public:
	// constructor
	RandomGenerator(long _seed=0)
	{
		if (_seed == 0)
			seed = (long) time(0);
		else
			seed = _seed;
	}

    // generate a random floating point number in the range (0.0, 1.0)
	double randf(void)
	{
		const long  a =      16807;  // multiplier
		const long  m = 2147483647;  // modulus
		const long  q =     127773;  // m div a
		const long  r =       2836;  // m mod a
		long        x_div_q;         // x divided by q
		long        x_mod_q;         // x modulo q
		long        x_new;           // new x value
		// RNG using integer arithmetic
		x_div_q = seed / q;
		x_mod_q = seed % q;
		x_new = (a * x_mod_q) - (r * x_div_q);
		if (x_new > 0)
			seed = x_new;
		else
			seed = x_new + m;
		// return a random value between 0.0 and 1.0
		return ((double)seed/m);
	}

    // generate a random floating point in the range [a, b]
	double uniform(double a, double b)
	{
		double res;
		do {
			if (a>b)
				throw RandomGeneratorError("Uniform argument rrror: a > b");
			res = (a+(b-a)*randf());
		}
		while (res<=0);
		return res;
	}

    // generate a uniformly distributed random integer in the interval [i..i+N]
	int random(int i, int n)
	{
		if (i>n)
			throw RandomGeneratorError("Random argument error: i > n");
		n -= i;
		n = int((n+1.0)*randf());
		return (i+n);
	}

    /** 
     *  Generate a random floating point number generated according to
     *  an exponential distribution with a given mean x.
     */ 
	double expntl(double x)
	{
		double res;
		do {
			res = -x*log(randf());
		}
		while (res <= 0);
		return res;
		//return(-x*log(randf()));
	}

    /** 
     *  Generate a random floating point number generated according to
     *  a normal distribution with a given mean x and standard deviation s.
     */ 
	double normal(double x, double s)
	{
		double v1,v2,w,z1;
		double res;
		do {
			static double z2=0.0;
			if (z2 != 0.0) {
				z1 = z2; // use value from previous call
				z2 = 0.0;
			}
			else {
				do {
					v1 = 2.0*randf()-1.0;
					v2 = 2.0*randf()-1.0;
					w = v1*v1+v2*v2;
				}
				while (w>=1.0);
				w = sqrt((-2.0*log(w))/w);
				z1 = v1*w;
				z2 = v2*w;
			}
			res = x+z1*s;
		} 
		while (res<=0);
		return res;
	}
};

#endif
