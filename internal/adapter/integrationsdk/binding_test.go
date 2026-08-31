package integrationsdkadapter

import (
	"errors"
	"testing"
)

type conversionFixture struct {
	Key   string   `json:"key"`
	Items []string `json:"items"`
}

func TestConvertReturnsDecodedContractValue(t *testing.T) {
	want := conversionFixture{Key: "connector", Items: []string{"send", "query"}}
	got, err := convert[conversionFixture](want, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Key != want.Key || len(got.Items) != len(want.Items) || got.Items[0] != want.Items[0] || got.Items[1] != want.Items[1] {
		t.Fatalf("converted value = %#v, want %#v", got, want)
	}
}

func TestConvertPreservesSourceError(t *testing.T) {
	want := errors.New("source failed")
	got, err := convert[conversionFixture](conversionFixture{Key: "ignored"}, want)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if got.Key != "" || got.Items != nil {
		t.Fatalf("value = %#v, want zero value", got)
	}
}
