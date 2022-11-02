package gocql

func (h *HostInfo) SetDatacenter(dc string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dataCenter = dc
}
