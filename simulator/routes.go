package simulator

import (
	"encoding/json"
	"net/http"
)

func (s *Simulator) RegisterAdminHandlers() {
	if s.Dashboard == nil {
		return
	}
	cli := s.Client()
	s.Dashboard.AddHandler("/api/admin/send", s.adminSend(cli))
	s.Dashboard.AddHandler("/api/admin/rules", s.adminRules(cli))
	s.Dashboard.AddHandler("/api/admin/rules/", s.adminRulesDetail(cli))
	s.Dashboard.AddHandler("/api/admin/services", s.adminServices(cli))
	s.Dashboard.AddHandler("/api/admin/lanes", s.adminLanes(cli))
	s.Dashboard.AddHandler("/api/admin/validate", s.adminValidate(cli))
	s.Dashboard.AddHandler("/api/admin/health", s.adminHealth(cli))
	s.Dashboard.AddHandler("/api/admin/partitions", s.adminPartitions(cli))
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, msg string, code int) {
	http.Error(w, msg, code)
}
