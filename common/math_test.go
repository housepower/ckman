package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMd5CheckSum(t *testing.T) {
	fmt.Println(Md5CheckSum("[abc]select * from system.tables\n"))
}

func TestMap_Union(t *testing.T) {
	m1 := Map{
		"kk1": "vv1",
		"kk2": "vv2",
		"kk3": "vv3",
	}
	m2 := Map{
		"kk2": "vv2",
		"kk3": "vv33",
		"kk5": "vv5",
	}

	m := m1.Union(m2)
	result := m.(Map)
	expect := Map{
		"kk1": "vv1",
		"kk2": "vv2",
		"kk3": "vv3",
		"kk5": "vv5",
	}
	assert.Equal(t, expect, result)
}

func TestMap_Union2(t *testing.T) {
	m1 := Map{}
	m2 := Map{
		"kk1": "vv1",
		"kk2": "vv2",
		"kk3": "vv3",
	}
	m3 := Map{
		"kk2": "vv2",
		"kk3": "vv33",
		"kk5": "vv5",
	}

	m1 = m1.Union(m2).(Map).Union(m3).(Map)
	expect := Map{
		"kk1": "vv1",
		"kk2": "vv2",
		"kk3": "vv3",
		"kk5": "vv5",
	}
	assert.Equal(t, expect, m1)
}

func TestMap_Intersect(t *testing.T) {
	m1 := Map{
		"kk1": "vv1",
		"kk2": "vv2",
		"kk3": "vv3",
	}
	m2 := Map{
		"kk2": "vv2",
		"kk3": "vv33",
		"kk5": "vv5",
	}

	m := m1.Intersect(m2)
	result := m.(Map)
	expect := Map{
		"kk2": "vv2",
		"kk3": "vv3",
	}
	assert.Equal(t, expect, result)
}

func TestMap_Difference(t *testing.T) {
	m1 := Map{
		"kk1": "vv1",
		"kk2": "vv2",
		"kk3": "vv3",
	}
	m2 := Map{
		"kk2": "vv2",
		"kk3": "vv33",
		"kk5": "vv5",
	}

	m := m1.Difference(m2)
	result := m.(Map)
	expect := Map{
		"kk1": "vv1",
	}
	assert.Equal(t, expect, result)
}

func TestMap_Difference2(t *testing.T) {
	m1 := Map{
		"kk1": "vv1",
		"kk2": "vv2",
		"kk3": "vv3",
	}
	m2 := Map{
		"kk2": "vv2",
		"kk3": "vv33",
		"kk5": "vv5",
	}

	m := m2.Difference(m1)
	result := m.(Map)
	expect := Map{
		"kk5": "vv5",
	}
	assert.Equal(t, expect, result)
}

func TestDecimal(t *testing.T) {
	assert.Equal(t, 0.46, Decimal(0.460003))
	assert.Equal(t, 0.47, Decimal(0.468999))
}

func TestArraySearch(t *testing.T) {
	assert.Equal(t, true, ArraySearch("bbb", []string{
		"aaa", "bbb", "ccc", "kkk",
	}))
	assert.Equal(t, false, ArraySearch("mmm", []string{
		"aaa", "bbb", "ccc", "kkk",
	}))
}

func TestArrayRemove(t *testing.T) {
	assert.Equal(t, []string{"aaa", "ccc", "kkk"}, ArrayRemove([]string{
		"aaa", "bbb", "ccc", "kkk",
	}, "bbb"))
	assert.Equal(t, []int{1, 2, 3, 4}, ArrayRemove([]int{
		1, 2, 3, 4, 5,
	}, 5))

	arr := []string{
		"aaa", "bbb", "ccc", "kkk",
	}
	ArrayRemove(arr, "bbb")
	assert.Equal(t, []string{"aaa", "bbb", "ccc", "kkk"}, arr)
}
