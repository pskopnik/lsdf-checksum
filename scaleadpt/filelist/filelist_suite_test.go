package filelist_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestFilelist(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Filelist Suite")
}
