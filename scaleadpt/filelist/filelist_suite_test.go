package filelist_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestFilelist(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Filelist Suite")
}
