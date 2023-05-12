package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGosypt_EnsurePassword(t *testing.T) {
	p1 := "123456"
	p2 := "ENC(E99D423889FBD0C4CF229E14D0864F68)"
	assert.Equal(t, p1, Gsypt.ensurePassword(p1))
	assert.Equal(t, "User@123", Gsypt.ensurePassword(p2))
}

type Extras struct {
	Name   string
	Passwd string
}
type ConfigTest struct {
	Id       int64
	Name     string
	Password string
	Extras   []Extras
	Hobby    []string
	Num      []int
	Mp       map[string]string
	MapStr   map[string]Extras
	In       Extras
}

func TestGosypt_Unmarshal(t *testing.T) {
	c := ConfigTest{
		Id:       1,
		Name:     "zhangsan",
		Password: "ENC(E99D423889FBD0C4CF229E14D0864F68)",
		Extras: []Extras{
			{Name: "root", Passwd: "ENC(E310E892E56801CED9ED98AA177F18E6)"},
			{Name: "root", Passwd: "123456"},
		},
		Hobby: []string{"ENC(E99D423889FBD0C4CF229E14D0864F68)", "123456"},
		Num:   []int{1, 2, 3, 4, 5},
		Mp: map[string]string{
			"name":     "zhangsan",
			"password": "ENC(E310E892E56801CED9ED98AA177F18E6)",
		},
		MapStr: map[string]Extras{
			"foo": {
				"zhangsan",
				"ENC(E310E892E56801CED9ED98AA177F18E6)",
			},
		},
		In: Extras{
			"zhangsan",
			"ENC(E310E892E56801CED9ED98AA177F18E6)",
		},
	}
	err := Gsypt.Unmarshal(&c)
	assert.Nil(t, err)
	fmt.Printf("%#v\n", c)
}

func TestSetAttribution(t *testing.T) {
	Gsypt.SetAttribution("<", ">", GosyptAlgorithm)
	assert.Equal(t, "123456", Gsypt.ensurePassword("123456"))
	assert.Equal(t, "ENC(123456)", Gsypt.ensurePassword("ENC(123456)"))
	assert.Equal(t, "User@123", Gsypt.ensurePassword("<E99D423889FBD0C4CF229E14D0864F68>"))
}
