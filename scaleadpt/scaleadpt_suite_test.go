package scaleadpt

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestScaleadpt(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Scaleadpt Suite")
}
