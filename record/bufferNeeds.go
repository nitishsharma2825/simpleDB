package record

import "math"

/*
static methods which estimate the optimal no of buffers to allocate for a scan
*/

/*
Considers various roots of the specified output size (in blocks)
and returns the highest root < number of available buffers
*/
func BestRoot(available int, size int) int {
	aval := available - 2 // reserve a couple
	if aval <= 1 {
		return 1
	}
	k := math.MaxInt
	i := 1.0
	for k > aval {
		i++
		k = int(math.Ceil(math.Pow(float64(size), 1/i)))
	}
	return k
}

/*
Considers various factors of the specified output size (in blocks)
returns the highest factor < number of available buffers
*/
func BestFactor(available int, size int) int {
	avail := available - 2
	if avail <= 1 {
		return 1
	}
	k := size
	i := 1.0
	for k > avail {
		i++
		k = int(math.Ceil(float64(size) / i))
	}
	return k
}
