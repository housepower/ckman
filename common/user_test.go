package common

import (
	"strings"
	"testing"
)

func TestGenerateInitialAdminPassword(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 32; i++ {
		password, err := GenerateInitialAdminPassword()
		if err != nil {
			t.Fatal(err)
		}
		if len(password) != 24 || seen[password] {
			t.Fatal("initial password has incorrect length or repeated")
		}
		seen[password] = true
		for _, alphabet := range []string{
			"abcdefghjkmnpqrstuvwxyz", "ABCDEFGHJKMNPQRSTUVWXYZ", "23456789", "!@#$%*-_+",
		} {
			if !strings.ContainsAny(password, alphabet) {
				t.Fatalf("password lacks required character category")
			}
		}
	}
}
