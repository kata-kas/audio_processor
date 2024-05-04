package security

import (
	"encoding/pem"
	"errors"
	"os"
)

func GetCertificate() (*[]byte, error) {
	if _, err := os.Stat("letsencrypt/cert.pem"); os.IsNotExist(err) {
		return nil, errors.New("certificate does not exist")
	}

	pemData, err := os.ReadFile("letsencrypt/cert.pem")
	if err != nil {
		return nil, errors.New("could not read certificate")
	}

	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, errors.New("failed to parse certificate PEM")
	}

	return &block.Bytes, nil
}
