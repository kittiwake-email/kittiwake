// Package metrics defines Kittiwake's Prometheus metrics in one place.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

var (
	// Deliveries counts LMTP/import deliveries by result.
	Deliveries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kittiwake", Subsystem: "delivery", Name: "messages_total", Help: "Messages delivered or refused, by result.",
	}, []string{"result"})
	// DeliveryBytes counts bytes of delivered messages.
	DeliveryBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "kittiwake", Subsystem: "delivery", Name: "bytes_total", Help: "Bytes of delivered messages.",
	})
	// Auth counts authentication attempts by protocol and result.
	Auth = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kittiwake", Subsystem: "auth", Name: "attempts_total", Help: "Authentication attempts by protocol and result.",
	}, []string{"protocol", "result"})
	// Connections tracks open protocol connections.
	Connections = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "kittiwake", Name: "connections", Help: "Open connections by protocol.",
	}, []string{"protocol"})
	// HTTPRequests counts API requests by handler and status class.
	HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kittiwake", Subsystem: "http", Name: "requests_total", Help: "HTTP requests by handler and status.",
	}, []string{"handler", "status"})
	// RadosOps counts RADOS operations by op and result.
	RadosOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kittiwake", Subsystem: "rados", Name: "ops_total", Help: "RADOS operations by op and result.",
	}, []string{"op", "result"})
	// ProxySessions counts logins handled by the proxy mode by protocol
	// and outcome (routed, unknown-domain, disabled, no-backend, refused,
	// error).
	ProxySessions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kittiwake", Subsystem: "proxy", Name: "sessions_total", Help: "Proxy logins by protocol and outcome.",
	}, []string{"protocol", "outcome"})
	// Tasks counts worker task outcomes by type.
	Tasks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kittiwake", Subsystem: "worker", Name: "tasks_total", Help: "Worker tasks by type and result.",
	}, []string{"type", "result"})
)

// Registry is the registry all handlers serve.
var Registry = prometheus.NewRegistry()

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		Deliveries, DeliveryBytes, Auth, Connections, HTTPRequests, RadosOps, ProxySessions, Tasks,
	)
}

// Handler serves /metrics.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{})
}
