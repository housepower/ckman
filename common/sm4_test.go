package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSM4OriginalKeyFromHexEnv(t *testing.T) {
	t.Setenv(SM4KeyEnv, "6368656e79616e6368616e6702000205")
	key, err := sm4Key()
	require.NoError(t, err)
	require.Equal(t, []byte{'c', 'h', 'e', 'n', 'y', 'a', 'n', 'c', 'h', 'a', 'n', 'g', 0x02, 0x00, 0x02, 0x05}, key)
	ciphertext, err := SM4Encrypt("test")
	require.NoError(t, err)
	plain, err := SM4Decrypt(ciphertext)
	require.NoError(t, err)
	require.Equal(t, "test", plain)
}

func TestSM4ConfigSecrets(t *testing.T) {
	t.Setenv(SM4KeyEnv, "1234567890abcdef")
	first, err := SM4Encrypt("secret-value")
	require.NoError(t, err)
	second, err := SM4Encrypt("secret-value")
	require.NoError(t, err)
	require.NotEqual(t, first, second)
	got, err := SM4Decrypt(first)
	require.NoError(t, err)
	require.Equal(t, "secret-value", got)

	g := &Gosypt{prefix: GosyptPrefixDefault, suffix: GosyptSuffxiDefault}
	require.NoError(t, g.SetAlgorithm(GosyptSM4Algorithm))
	cfg := struct{ Password string }{Password: "ENC(" + first + ")"}
	require.NoError(t, g.Unmarshal(&cfg))
	require.Equal(t, "secret-value", cfg.Password)

	nested := map[string]interface{}{"database": map[string]interface{}{"password": "ENC(" + first + ")"}}
	require.NoError(t, g.Unmarshal(&nested))
	require.Equal(t, "secret-value", nested["database"].(map[string]interface{})["password"])

	cfg.Password = "ENC(corrupted)"
	require.Error(t, g.Unmarshal(&cfg))
	t.Setenv(SM4KeyEnv, "wrong-length")
	require.Error(t, g.SetAlgorithm(GosyptSM4Algorithm))
	require.Error(t, g.Unmarshal(&cfg))
}
