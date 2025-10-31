//go:build windows

package main

import (
	_ "embed"
)

//go:embed rsrc_windows.syso
var windowsResources []byte