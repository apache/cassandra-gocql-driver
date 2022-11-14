package scyllacloud

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type ConnectionConfig struct {
	// Kind is a string value representing the REST resource this object represents.
	// Servers may infer this from the endpoint the client submits requests to.
	// In CamelCase.
	// +optional
	Kind string `json:"kind,omitempty"`
	// APIVersion defines the versioned schema of this representation of an object.
	// Servers should convert recognized schemas to the latest internal value, and
	// may reject unrecognized values.
	// +optional
	APIVersion string `json:"apiVersion,omitempty"`
	// Datacenters is a map of referencable names to datacenter configs.
	Datacenters map[string]*Datacenter `json:"datacenters"`
	// AuthInfos is a map of referencable names to authentication configs.
	AuthInfos map[string]*AuthInfo `json:"authInfos"`
	// Contexts is a map of referencable names to context configs.
	Contexts map[string]*Context `json:"contexts"`
	// CurrentContext is the name of the context that you would like to use by default.
	CurrentContext string `json:"currentContext"`
	// Parameters is a struct containing common driver configuration parameters.
	// +optional
	Parameters *Parameters `json:"parameters,omitempty"`
}

type AuthInfo struct {
	// ClientCertificateData contains PEM-encoded data from a client cert file for TLS. Overrides ClientCertificatePath.
	// +optional
	ClientCertificateData []byte `json:"clientCertificateData,omitempty"`
	// ClientCertificatePath is the path to a client cert file for TLS.
	// +optional
	ClientCertificatePath string `json:"clientCertificatePath,omitempty"`
	// ClientKeyData contains PEM-encoded data from a client key file for TLS. Overrides ClientKeyPath.
	// +optional
	ClientKeyData []byte `json:"clientKeyData,omitempty"`
	// ClientKeyPath is the path to a client key file for TLS.
	// +optional
	ClientKeyPath string `json:"clientKeyPath,omitempty"`
	// Username is the username for basic authentication to the Scylla cluster.
	// +optional
	Username string `json:"username,omitempty"`
	// Password is the password for basic authentication to the Scylla cluster.
	// +optional	`
	Password string `json:"password,omitempty"`
}

type Datacenter struct {
	// CertificateAuthorityPath is the path to a cert file for the certificate authority.
	// +optional
	CertificateAuthorityPath string `json:"certificateAuthorityPath,omitempty"`
	// CertificateAuthorityData contains PEM-encoded certificate authority certificates. Overrides CertificateAuthority.
	// +optional
	CertificateAuthorityData []byte `json:"certificateAuthorityData,omitempty"`
	// Server is the initial contact point of the Scylla cluster.
	// Example: https://hostname:port
	Server string `json:"server"`
	// TLSServerName is used to check server certificates. If TLSServerName is empty, the hostname used to contact the server is used.
	// +optional
	TLSServerName string `json:"tlsServerName,omitempty"`
	// NodeDomain the domain suffix that is concatenated with host_id of the node driver wants to connect to.
	// Example: host_id.<nodeDomain>
	NodeDomain string `json:"nodeDomain"`
	// InsecureSkipTLSVerify skips the validity check for the server's certificate. This will make your HTTPS connections insecure.
	// +optional
	InsecureSkipTLSVerify bool `json:"insecureSkipTlsVerify,omitempty"`
	// ProxyURL is the URL to the proxy to be used for all requests made by this
	// client. URLs with "http", "https", and "socks5" schemes are supported. If
	// this configuration is not provided or the empty string, the client
	// attempts to construct a proxy configuration from http_proxy and
	// https_proxy environment variables. If these environment variables are not
	// set, the client does not attempt to proxy requests.
	// +optional
	ProxyURL string `json:"proxyUrl,omitempty"`
}

type Context struct {
	// DatacenterName is the name of the datacenter for this context.
	DatacenterName string `json:"datacenterName"`
	// AuthInfoName is the name of the authInfo for this context.
	AuthInfoName string `json:"authInfoName"`
}

type Parameters struct {
	// DefaultConsistency is the default consistency level used for user queries.
	// +optional
	DefaultConsistency ConsistencyString `json:"defaultConsistency,omitempty"`
	// DefaultSerialConsistency is the default consistency level for the serial part of user queries.
	// +optional
	DefaultSerialConsistency ConsistencyString `json:"defaultSerialConsistency,omitempty"`
}

type ConsistencyString string

// just AnyConsistency etc is better, but there's already SerialConsistency defined elsewhere.
const (
	DefaultAnyConsistency         ConsistencyString = "ANY"
	DefaultOneConsistency         ConsistencyString = "ONE"
	DefaultTwoConsistency         ConsistencyString = "TWO"
	DefaultThreeConsistency       ConsistencyString = "THREE"
	DefaultQuorumConsistency      ConsistencyString = "QUORUM"
	DefaultAllConsistency         ConsistencyString = "ALL"
	DefaultLocalQuorumConsistency ConsistencyString = "LOCAL_QUORUM"
	DefaultEachQuorumConsistency  ConsistencyString = "EACH_QUORUM"
	DefaultSerialConsistency      ConsistencyString = "SERIAL"
	DefaultLocalSerialConsistency ConsistencyString = "LOCAL_SERIAL"
	DefaultLocalOneConsistency    ConsistencyString = "LOCAL_ONE"
)

func (cc *ConnectionConfig) GetRootCAPool() (*x509.CertPool, error) {
	caPool := x509.NewCertPool()

	for dcName, dc := range cc.Datacenters {
		if len(dc.CertificateAuthorityData) == 0 && len(dc.CertificateAuthorityPath) == 0 {
			return nil, fmt.Errorf("datacenter %q does not include certificate authority", dcName)
		}

		caData, err := cc.getDataOrReadFile(dc.CertificateAuthorityData, dc.CertificateAuthorityPath)
		if err != nil {
			return nil, fmt.Errorf("can't read datacenter %q certificate authority file from %q: %w", dcName, dc.CertificateAuthorityPath, err)
		}

		caPool.AppendCertsFromPEM(caData)
	}

	return caPool, nil
}

func (cc *ConnectionConfig) GetDatacenterCAPool(datacenterName string) (*x509.CertPool, error) {
	dc, ok := cc.Datacenters[datacenterName]
	if !ok {
		return nil, fmt.Errorf("datacenter %q not found in cloud connection config", datacenterName)
	}

	caPool := x509.NewCertPool()

	if len(dc.CertificateAuthorityData) == 0 && len(dc.CertificateAuthorityPath) == 0 {
		return nil, fmt.Errorf("datacenter %q does not include certificate authority", datacenterName)
	}

	caData, err := cc.getDataOrReadFile(dc.CertificateAuthorityData, dc.CertificateAuthorityPath)
	if err != nil {
		return nil, fmt.Errorf("can't read datacenter %q certificate authority file from %q: %w", datacenterName, dc.CertificateAuthorityPath, err)
	}
	caPool.AppendCertsFromPEM(caData)

	return caPool, nil
}

func (cc *ConnectionConfig) GetInitialContactPoints() []string {
	hosts := make([]string, 0, len(cc.Datacenters))
	for _, dc := range cc.Datacenters {
		hosts = append(hosts, dc.Server)
	}
	return hosts
}

func (cc *ConnectionConfig) getDataOrReadFile(data []byte, path string) ([]byte, error) {
	if len(data) == 0 {
		return os.ReadFile(path)
	}

	return data, nil
}

func (cc *ConnectionConfig) GetCurrentDatacenterConfig() (*Datacenter, error) {
	contextConf, err := cc.GetCurrentContextConfig()
	if err != nil {
		return nil, fmt.Errorf("can't get current context config: %w", err)
	}

	if len(contextConf.DatacenterName) == 0 {
		return nil, fmt.Errorf("datacenterName in current context can't be empty")
	}

	dcConf, ok := cc.Datacenters[contextConf.DatacenterName]
	if !ok {
		return nil, fmt.Errorf("datacenter %q does not exists", contextConf.DatacenterName)
	}

	return dcConf, nil
}

func (cc *ConnectionConfig) GetCurrentContextConfig() (*Context, error) {
	if len(cc.CurrentContext) == 0 {
		return nil, fmt.Errorf("current context can't be empty")
	}

	contextConf, ok := cc.Contexts[cc.CurrentContext]
	if !ok {
		return nil, fmt.Errorf("context %q does not exists", cc.CurrentContext)
	}

	return contextConf, nil
}

func (cc *ConnectionConfig) GetCurrentAuthInfo() (*AuthInfo, error) {
	contextConf, err := cc.GetCurrentContextConfig()
	if err != nil {
		return nil, fmt.Errorf("can't get current context config: %w", err)
	}

	if len(contextConf.AuthInfoName) == 0 {
		return nil, fmt.Errorf("authInfo in current context can't be empty")
	}

	authInfo, ok := cc.AuthInfos[contextConf.AuthInfoName]
	if !ok {
		return nil, fmt.Errorf("authInfo %q does not exists", contextConf.AuthInfoName)
	}

	return authInfo, nil
}

func (cc *ConnectionConfig) GetClientCertificate() (*tls.Certificate, error) {
	authInfo, err := cc.GetCurrentAuthInfo()
	if err != nil {
		return nil, fmt.Errorf("can't get current auth info: %w", err)
	}

	clientCert, err := cc.getDataOrReadFile(authInfo.ClientCertificateData, authInfo.ClientCertificatePath)
	if err != nil {
		return nil, fmt.Errorf("can't read client certificate: %w", err)
	}

	clientKey, err := cc.getDataOrReadFile(authInfo.ClientKeyData, authInfo.ClientKeyPath)
	if err != nil {
		return nil, fmt.Errorf("can't read client key: %w", err)
	}

	cert, err := tls.X509KeyPair(clientCert, clientKey)
	if err != nil {
		return nil, fmt.Errorf("can't create x509 pair: %w", err)
	}

	return &cert, nil
}
