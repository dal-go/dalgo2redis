package dalgo2redis

import (
	"reflect"
	"strings"
	"testing"
)

func TestScopedScanAdmission(t *testing.T) {
	options := Options{ScanPrefix: "tenant[*?\\]:"}
	wantPattern := "tenant\\[\\*\\?\\\\\\]:*"
	got, err := prepareCommand([]string{"scan", "18446744073709551615", "count", "10", "type", "string"}, options)
	want := []string{"SCAN", "18446744073709551615", "COUNT", "10", "TYPE", "string", "MATCH", wantPattern}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("admitted %q, %v; want %q", got, err, want)
	}
	if err := ValidateCommand([]string{"SCAN", "0"}); err == nil {
		t.Fatal("SCAN without configured scope admitted")
	}
	for _, args := range [][]string{
		{"SCAN", "0", "MATCH", "*"}, {"SCAN", "0", "MATCH", "tenant*"},
		{"SCAN", "0", "MATCH", wantPattern, "MATCH", wantPattern},
		{"SCAN", "0", "COUNT", "1", "count", "2"}, {"SCAN", "0", "COUNT", "0"},
		{"SCAN", "0", "COUNT", "1001"}, {"SCAN", "0", "COUNT", "-1"},
		{"SCAN", "+1"}, {"SCAN", "18446744073709551616"},
		{"SCAN", "0", "COUNT"}, {"SCAN", "0", "TYPE", ""},
		{"SCAN", "0", "SOMETHING", "x"}, {"KEYS", "*"}, {"SET", "x", "y"},
		{"MULTI", "x"}, {"EVAL", "x", "0"},
	} {
		if err := ValidateCommandWithOptions(args, options); err == nil {
			t.Fatalf("unsafe command admitted: %q", args)
		}
	}
	input := []string{"SCAN", "0", "MATCH", wantPattern}
	copied, err := prepareCommand(input, options)
	if err != nil {
		t.Fatal(err)
	}
	input[1] = "9"
	if copied[1] != "0" {
		t.Fatal("caller mutation changed admitted command")
	}
	if err := ValidateCommand([]string{"ZSCORE", "z", "member"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCommand([]string{"ZSCORE", "z"}); err == nil {
		t.Fatal("invalid ZSCORE arity")
	}
}

func TestScanAdmissionRetainsByteBudgets(t *testing.T) {
	if _, err := prepareCommand([]string{"SCAN", "0"}, Options{ScanPrefix: strings.Repeat("*", 40000)}); err == nil {
		t.Fatal("escaped prefix bypassed 64 KiB command budget")
	}
	if _, err := prepareCommand([]string{"SCAN", "0"}, Options{ScanPrefix: "\xff:"}); err != nil {
		t.Fatal("literal byte prefix was not retained")
	}
}

func TestScanReplyShapeAndScope(t *testing.T) {
	for _, page := range []any{
		nil, []any{"0"}, []any{"-1", []any{}},
		[]any{"18446744073709551616", []any{}},
		[]any{int64(0), []any{}}, []any{"0", nil},
		[]any{"0", []any{"tenant:a", "foreign-secret"}},
		[]any{"0", []any{int64(1)}},
	} {
		if err := validateScanReply(page, "tenant:"); err == nil {
			t.Fatalf("unsafe page accepted: %#v", page)
		} else if strings.Contains(err.Error(), "foreign-secret") {
			t.Fatal("out-of-scope key leaked through diagnostic")
		}
	}
	// Empty nonterminal pages and duplicates are legal: this is not a snapshot.
	for _, page := range []any{
		[]any{"17", []any{}},
		[]any{"0", []any{"tenant:a", "tenant:a"}},
	} {
		if err := validateScanReply(page, "tenant:"); err != nil {
			t.Fatal(err)
		}
	}
}
