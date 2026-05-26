package client

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func buildTestTLSConfig(t *testing.T) *tls.Config {
	t.Helper()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER}))

	certKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caCertParsed, err := x509.ParseCertificate(caCertDER)
	require.NoError(t, err)

	certTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "longhorn-backend.longhorn-system"},
		DNSNames:     []string{"longhorn-backend.longhorn-system"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, certTemplate, caCertParsed, &certKey.PublicKey, caKey)
	require.NoError(t, err)

	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(certKey)}),
	)
	require.NoError(t, err)

	return &tls.Config{
		MinVersion:    tls.VersionTLS13,
		Renegotiation: tls.RenegotiateNever,
		ServerName:    "longhorn-backend.longhorn-system",
		RootCAs:       pool,
		Certificates:  []tls.Certificate{cert},
	}
}

func TestNewSPDKClientWithTLSConfig_WithTLS(t *testing.T) {
	tlsConfig := buildTestTLSConfig(t)

	client, err := NewSPDKClientWithTLSConfig("localhost:8504", tlsConfig)

	require.NoError(t, err)
	require.NotNil(t, client)
	require.Equal(t, "localhost:8504", client.serviceURL)
	require.NoError(t, client.Close())
}

func TestNewSPDKClientWithTLSConfig_WithoutTLS(t *testing.T) {
	client, err := NewSPDKClientWithTLSConfig("localhost:8504", nil)

	require.NoError(t, err)
	require.NotNil(t, client)
	require.Equal(t, "localhost:8504", client.serviceURL)
	require.NoError(t, client.Close())
}
