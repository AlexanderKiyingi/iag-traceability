package models

// Permission catalogue — mirrors SCM traceability.* codenames for gateway compatibility.
type PermissionDescriptor struct {
	Name        string
	Description string
}

func PermissionDescriptors() []PermissionDescriptor {
	return []PermissionDescriptor{
		{"traceability.view_chain", "View chain of custody and trace events"},
		{"traceability.add_trace_event", "Append custody trace events"},
		{"traceability.publish_qr", "Publish or revoke public lot QR codes"},
		{"traceability.view_events", "List trace events (internal)"},
	}
}
