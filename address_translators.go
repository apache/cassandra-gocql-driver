package gocql

import "net"

// AddressTranslator provides a way to translate node addresses (and ports) that are
// discovered or received as a node event. This can be useful in an ec2 environment,
// for instance, to translate public IPs to private IPs.
// Deprecated: Use HostAddressTranslator
type AddressTranslator interface {
	// Translate will translate the provided address and/or port to another
	// address and/or port. If no translation is possible, Translate will return the
	// address and port provided to it.
	Translate(addr net.IP, port int) (net.IP, int)
}

// Deprecated: Use HostAddressTranslatorFunc
type AddressTranslatorFunc func(addr net.IP, port int) (net.IP, int)

func (fn AddressTranslatorFunc) Translate(addr net.IP, port int) (net.IP, int) {
	return fn(addr, port)
}

// IdentityTranslator will do nothing but return what it was provided. It is
// essentially a no-op.
// Deprecated: Use HostIdentityTranslator
func IdentityTranslator() AddressTranslator {
	return AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
		return addr, port
	})
}

// HostAddressTranslator provides a way to translate node addresses (and ports)
// that are discovered or received as a node event. This can be useful in an ec2
// environment, for instance, to translate public IPs to private IPs. The
// HostInfo is provided to allow for inspecting the various attributes of the
// node but depending on the context the HostInfo might not be filled in
// completely.
type HostAddressTranslator interface {
	// Translate will translate the provided address and/or port to another
	// address and/or port. If no translation is possible, Translate will return the
	// address and port provided to it.
	Translate(addr net.IP, port int, host *HostInfo) (net.IP, int)
}

type HostAddressTranslatorFunc func(addr net.IP, port int, host *HostInfo) (net.IP, int)

func (fn HostAddressTranslatorFunc) Translate(addr net.IP, port int, host *HostInfo) (net.IP, int) {
	return fn(addr, port, host)
}

// IdentityTranslator will do nothing but return what it was provided. It is essentially a no-op.
func HostIdentityTranslator() HostAddressTranslator {
	return HostAddressTranslatorFunc(func(addr net.IP, port int, host *HostInfo) (net.IP, int) {
		return addr, port
	})
}
