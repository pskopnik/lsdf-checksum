package scaleadpt

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestScaleadpt(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Scaleadpt Suite")
}
