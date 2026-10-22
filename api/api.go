// Package api embeds the OpenAPI specification of the REST endpoints so the
// running daemon can serve the exact contract it implements.
package api

import _ "embed"

// OpenAPI is the YAML specification of the REST (admin, OAuth, self-service)
// endpoints. The admin listener serves it at /openapi.yaml.
//
//go:embed openapi.yaml
var OpenAPI []byte
