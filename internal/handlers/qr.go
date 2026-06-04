package handlers

import (
	"github.com/skip2/go-qrcode"
)

func qrPNG(url string) ([]byte, error) {
	return qrcode.Encode(url, qrcode.Medium, 256)
}
