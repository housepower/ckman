package common

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
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
