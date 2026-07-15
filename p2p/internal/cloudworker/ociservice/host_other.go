//go:build !linux

package ociservice

func NewProductionDriver(DescriptorResolver) (*Driver, error) {
	return nil, ErrProductionHost
}

func validateProductionSecretFiles(spec ContainerSpec) error {
	if len(spec.SecretMounts) == 0 {
		return nil
	}
	return ErrProductionHost
}
