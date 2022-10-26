/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import "fmt"

const queue = 3

func main() {
	arr := []int{1, 161, 2, 5, 215, 215, 3, 4, 231, 398}
	canThreePartsEqualSum(arr)
}

func canThreePartsEqualSum(A []int) {
	sum := OriginSUM(A)
	length := len(A)

	var (
		devide int
		r1     []int
		r2     []int
		r3     []int
	)

	devide = sum / queue
	fmt.Printf("arr sum [%v] devide [%v]\n", sum, devide)

	i := 0

	// 元素是否大于 devide，尽可能保证每个队列有个元素
	j := 0

	var a, b int
	for i < length {
		a += A[i]
		if j == 0 && a >= devide {
			r1 = append(r1, A[i])
			i++
			j++
		} else {
			if a >= devide {
				break
			}
			r1 = append(r1, A[i])
			i++
			j++
		}

	}

	j = 0

	for i < length {
		b += A[i]
		if j == 0 && b >= devide {
			r2 = append(r2, A[i])
			i++
			j++
		} else {
			if b >= devide {
				break
			}
			r2 = append(r2, A[i])
			i++
			j++
		}
	}

	j = 0
	r3 = append(r3, A[i:]...)

	fmt.Printf("r1 a values [%v] r1 values %v sum [%v]\n", a, r1, OriginSUM(r1))
	fmt.Printf("r2 b values [%v] r2 values %v sum [%v]\n", b, r2, OriginSUM(r2))
	fmt.Printf("r3 c values [%v] r3 values %v sum [%v]\n", OriginSUM(A[i:]), r3, OriginSUM(A[i:]))

}

func OriginSUM(s []int) int {
	var sum int
	l, r := 0, len(s)-1
	for l <= r {
		if l == r {
			sum += s[l]
			break
		}
		sum += (s[l] + s[r])
		l++
		r--
	}
	return sum
}
