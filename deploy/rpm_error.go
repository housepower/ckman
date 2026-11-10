package deploy

import (
	"strings"

	"github.com/pkg/errors"
)

func explainRPMInstallError(pkgType string, err error) error {
	if err == nil {
		return nil
	}

	if pkgType != "" && !strings.HasSuffix(pkgType, ".rpm") {
		return err
	}

	message := strings.ToLower(err.Error())
	hasScriptletStatus132 := strings.Contains(message, "132") &&
		(strings.Contains(message, "%post") || strings.Contains(message, "scriptlet") || strings.Contains(message, "脚本"))
	if !hasScriptletStatus132 &&
		!strings.Contains(message, "exit status 132") &&
		!strings.Contains(message, "illegal instruction") &&
		!strings.Contains(message, "非法指令") &&
		!strings.Contains(message, "signal 4") {
		return err
	}

	return errors.Wrap(
		err,
		"ClickHouse RPM installation was terminated by SIGILL (exit status 132); the ARM CPU or virtual machine does not expose the instruction set required by this package; use an ARM-compatible package or rebuild ClickHouse with -DNO_ARMV81_OR_HIGHER=1",
	)
}
