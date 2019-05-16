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
 *  This file implements the K-means algorithm based on the Lloyds algorithm
 *  that uses the kmeans++ initialization method. The implementation is available
 *  at the following URL: https://github.com/genbattle/dkm.
 */ 

#ifndef SQ_DKM_H
#define SQ_DKM_H

// includes
#include <tuple>
#include <array>
#include <cstddef>
#include <cstdint>
#include <random>
#include <vector>
#include <algorithm>
#include <type_traits>
#include <basic.hpp>
#include "random.hpp"

// defines
#define N_CENTROIDS 3

using namespace std;

// class implementing the set of Centroids
class Centroids
{
private:
    uint64_t wid; // unique identifier (starting from zero)
    vector<array<float, DIM>> centroids; // centroids of the clusters found

public:
    // constructor
    Centroids(): wid(0) {}

    // destructor
    ~Centroids() {}

    // getControlFields method
    pair<size_t, uint64_t> getControlFields() const
    {
        return pair<size_t, uint64_t>(0, wid);
    }

    // setControlFields method
    void setControlFields(size_t _key, uint64_t _wid)
    {
        wid = _wid;
    }

    // method to get the centroids
    vector<array<float, DIM>> getCentroids() const
    {
    	return centroids;
    }

    // method to set the centroids
    void setCentroids(vector<array<float, DIM>> _centroids)
    {
    	centroids = _centroids;
    }
};

// calculate the square of the distance between two points
template <typename T, size_t N>
T distance_squared(const array<T, N> &point_a, const array<T, N> &point_b)
{
	T d_squared = T();
	for (typename array<T, N>::size_type i=0; i<N; ++i) {
		auto delta = point_a[i] - point_b[i];
		d_squared += delta * delta;
	}
	return d_squared;
}

// function distance
template <typename T, size_t N>
T distance(const array<T, N> &point_a, const array<T, N> &point_b)
{
	return sqrt(distance_squared(point_a, point_b));
}

// calculate the smallest distance between each of the data points and any of the input means
template <typename T, size_t N>
vector<T> closest_distance(const vector<array<T, N>> &means, const vector<array<T, N>> &data)
{
	vector<T> distances;
	distances.reserve(data.size());
	for (auto &d: data) {
		T closest = distance_squared(d, means[0]);
		for (auto &m: means) {
			T distance = distance_squared(d, m);
			if (distance < closest)
				closest = distance;
		}
		distances.push_back(closest);
	}
	return distances;
}

// function implementing the initialization method (kmeans++)
template <typename T, size_t N>
vector<array<T, N>> random_plusplus(const vector<array<T, N>> &data, uint32_t k)
{
	assert(k > 0);
	using input_size_t = typename array<T, N>::size_type;
	vector<array<T, N>> means;
	// Using a very simple PRBS generator, parameters selected according to
	// https://en.wikipedia.org/wiki/Linear_congruential_generator#Parameters_in_common_use
	random_device rand_device;
	linear_congruential_engine<uint64_t, 6364136223846793005, 1442695040888963407, UINT64_MAX> rand_engine(rand_device());
	// select first mean at random from the set
	{
		uniform_int_distribution<input_size_t> uniform_generator(0, data.size() - 1);
		means.push_back(data[uniform_generator(rand_engine)]);
	}
	for (uint32_t count=1; count<k; ++count) {
		// calculate the distance to the closest mean for each data point
		auto distances = closest_distance(means, data);
		// pick a random point weighted by the distance from existing means
		// TODO: This might convert floating point weights to ints, distorting the distribution for small weights
#if !defined(_MSC_VER) || _MSC_VER >= 1900
		discrete_distribution<input_size_t> generator(distances.begin(), distances.end());
#else  // MSVC++ older than 14.0
		input_size_t i = 0;
		discrete_distribution<input_size_t> generator(distances.size(), 0.0, 0.0, [&distances, &i](double) { return distances[i++]; });
#endif
		means.push_back(data[generator(rand_engine)]);
	}
	return means;
}

// function to choose the initial centroids (simplified version to obtain a deterministic execution)
template <typename T, size_t N>
vector<array<T, N>> random_my(const vector<array<T, N>> &data, uint32_t k)
{
	assert(k > 0);
	RandomGenerator random(1);
	vector<size_t> chosen;
	vector<array<T, N>> means;
	while (chosen.size() < N_CENTROIDS) {
		size_t idx = random.random(0, data.size()-1);
		if (find(chosen.begin(), chosen.end(), idx) == chosen.end()) {
			means.push_back(data[idx]);
			chosen.push_back(idx);
		}
	}
	return means;
}

// calculate the index of the mean a particular data point is closest to (euclidean distance)
template <typename T, size_t N>
uint32_t closest_mean(const array<T, N> &point, const vector<array<T, N>> &means)
{
	assert(!means.empty());
	T smallest_distance = distance_squared(point, means[0]);
	typename array<T, N>::size_type index = 0;
	T distance;
	for (size_t i=1; i<means.size(); ++i) {
		distance = distance_squared(point, means[i]);
		if (distance < smallest_distance) {
			smallest_distance = distance;
			index = i;
		}
	}
	return index;
}

// calculate the index of the mean each data point is closest to (euclidean distance)
template <typename T, size_t N>
vector<uint32_t> calculate_clusters(const vector<array<T, N>> &data, const vector<array<T, N>> &means)
{
	vector<uint32_t> clusters;
	for (auto &point: data) {
		clusters.push_back(closest_mean(point, means));
	}
	return clusters;
}

// calculate means based on data points and their cluster assignments
template <typename T, size_t N>
vector<array<T, N>> calculate_means(const vector<array<T, N>> &data, const vector<uint32_t> &clusters, const vector<array<T, N>> &old_means, uint32_t k)
{
	vector<array<T, N>> means(k);
	vector<T> count(k, T());
	for (size_t i=0; i<min(clusters.size(), data.size()); ++i) {
		auto &mean = means[clusters[i]];
		count[clusters[i]] += 1;
		for (size_t j=0; j<min(data[i].size(), mean.size()); ++j) {
			mean[j] += data[i][j];
		}
	}
	for (size_t i=0; i<k; ++i) {
		if (count[i] == 0) {
			means[i] = old_means[i];
		}
		else {
			for (size_t j=0; j<means[i].size(); ++j) {
				means[i][j] /= count[i];
			}
		}
	}
	return means;
}

/*  
 *  Implementation of k-means generic across the data type and the dimension of each data item. Expects
 *  the data to be a vector of fixed-size arrays. Generic parameters are the type of the base data (T)
 *  and the dimensionality of each data point (N). All points must have the same dimensionality.
 *  Returns a std::tuple containing:
 *   0: a vector holding the means for each cluster from 0 to k-1;
 *   1: a vector containing the cluster number (0 to k-1) for each corresponding element of the input
 *	  data vector.
 *  Implementation details:
 *  This implementation of k-means uses [Lloyd's Algorithm](https://en.wikipedia.org/wiki/Lloyd%27s_algorithm)
 *  with the [kmeans++](https://en.wikipedia.org/wiki/K-means%2B%2B)
 *  used for initializing the means.
 */
template <typename T, size_t N>
tuple<vector<array<T, N>>, vector<uint32_t>> kmeans_lloyd(const vector<array<T, N>> &data, uint32_t k)
{
	static_assert(is_arithmetic<T>::value && is_signed<T>::value, "kmeans_lloyd requires the template parameter T to be a signed arithmetic type (e.g. float, double, int)");
	assert(k > 0); // k must be greater than zero
	assert(data.size() >= k); // there must be at least k data points
	
	//vector<array<T, N>> means = random_plusplus(data, k);
	
	vector<array<T, N>> means = random_my(data, k);
	
	vector<array<T, N>> old_means;
	vector<uint32_t> clusters;
	// calculate new means until convergence is reached
	int count = 0;
	do {
		clusters = calculate_clusters(data, means);
		old_means = means;
		means = calculate_means(data, clusters, old_means, k);
		++count;
	} while (means != old_means);
	return tuple<vector<array<T, N>>, vector<uint32_t>>(means, clusters);
}

// user-defined function to compute the k-means algorithm given a set of tuples
size_t KmeansFunction(size_t key, size_t wid, Iterable<Skyline> &win, Centroids &result) {
	// compute the union of the skylines
	set<tuple_t> unionSet;
	for (size_t i=0; i<win.size(); i++)
		unionSet.insert(win[i].begin(), win[i].end());
	// prepare the input
	vector<array<float, DIM>> inputData;
	for (auto &_t: unionSet)
		inputData.push_back(_t.toArray());
	// compute k-means
	auto cluster_data = kmeans_lloyd(inputData, N_CENTROIDS); // K-means
	result.setCentroids(get<0>(cluster_data));
	return 0;
};

#endif
