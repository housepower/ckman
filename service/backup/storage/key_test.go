package storage

import "testing"

func TestValidateKeyPrefix_RejectsShellMetacharacters(t *testing.T) {
	// Codex review P1：local 后端用 keyPrefix 拼 ssh 命令，必须挡掉所有 shell 元字符
	bad := []string{
		"", "/abs", "a/../b", "a/./b", "a//b",
		"a;rm -rf /", "a$x", "a`whoami`", "a|cat", "a&b",
		`a"b`, "a'b", "a b", "a>b", "a<b", "a*b", "a?b", "a(b", "a)b",
		"a\\b", "a\nb",
	}
	for _, p := range bad {
		if err := validateKeyPrefix(p); err == nil {
			t.Errorf("validateKeyPrefix(%q) should reject", p)
		}
	}
	// 真实合法形态
	good := []string{
		"abc/20260304/db.table/192.168.1.1",
		"20260304/db.t/host-01",
		"cluster_1/20260304/db.t/h",
	}
	for _, p := range good {
		if err := validateKeyPrefix(p); err != nil {
			t.Errorf("validateKeyPrefix(%q) should accept, got %v", p, err)
		}
	}
}

func TestJoinRunKey(t *testing.T) {
	cases := []struct {
		name     string
		prefix   string
		want     string
	}{
		{"empty prefix (legacy)", "", "20260304/db1.t1/h1"},
		{"cluster prefix", "abc", "abc/20260304/db1.t1/h1"},
		{"prefix with trailing slash", "abc/", "abc/20260304/db1.t1/h1"},
		{"prefix with leading slash", "/abc", "abc/20260304/db1.t1/h1"},
		{"prefix with both slashes", "/abc/", "abc/20260304/db1.t1/h1"},
		{"prefix only slashes (degenerates to empty)", "///", "20260304/db1.t1/h1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := JoinRunKey(c.prefix, "20260304", "db1", "t1", "h1")
			if got != c.want {
				t.Errorf("JoinRunKey(%q,...) = %q, want %q", c.prefix, got, c.want)
			}
		})
	}
}
