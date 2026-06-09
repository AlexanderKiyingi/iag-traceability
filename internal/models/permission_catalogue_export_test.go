package models

import (
	"encoding/json"
	"os"
	"testing"
)

// TestExportPermissionCatalogue supports scripts/permission-catalogue-drift in the monorepo.
func TestExportPermissionCatalogue(t *testing.T) {
	if os.Getenv("EXPORT_PERM_CATALOGUE") != "1" {
		return
	}
	type row struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	rows := make([]row, 0, len(PermissionDescriptors()))
	for _, d := range PermissionDescriptors() {
		rows = append(rows, row{Name: d.Name, Description: d.Description})
	}
	b, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	out := os.Getenv("EXPORT_PERM_OUT")
	if out == "" {
		t.Fatal("EXPORT_PERM_OUT required")
	}
	if err := os.WriteFile(out, b, 0o644); err != nil {
		t.Fatal(err)
	}
}
