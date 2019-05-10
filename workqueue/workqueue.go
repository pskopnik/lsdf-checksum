package workqueue

const gocraftWorkNamespaceBase string = "lsdf-checksum/workqueue:work"

func GocraftWorkNamespace(prefix string) string {
	return prefix + gocraftWorkNamespaceBase
}
