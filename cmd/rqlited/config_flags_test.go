package main

import "testing"

func Test_ForgeAdmissionFlags(t *testing.T) {
	args := []string{
		"-write-concurrency-limit", "32",
		"-write-queue-limit", "8",
		"-read-concurrency-limit", "64",
		"-read-queue-limit", "16",
		"/tmp/data",
	}
	_, cfg, err := Forge(args)
	if err != nil {
		t.Fatalf("forge: %v", err)
	}
	if cfg.WriteConcurrencyLimit != 32 {
		t.Fatalf("WriteConcurrencyLimit: got %d want 32", cfg.WriteConcurrencyLimit)
	}
	if cfg.WriteQueueLimit != 8 {
		t.Fatalf("WriteQueueLimit: got %d want 8", cfg.WriteQueueLimit)
	}
	if cfg.ReadConcurrencyLimit != 64 {
		t.Fatalf("ReadConcurrencyLimit: got %d want 64", cfg.ReadConcurrencyLimit)
	}
	if cfg.ReadQueueLimit != 16 {
		t.Fatalf("ReadQueueLimit: got %d want 16", cfg.ReadQueueLimit)
	}
}

func Test_ForgeAdmissionFlagsDefaultZero(t *testing.T) {
	_, cfg, err := Forge([]string{"/tmp/data"})
	if err != nil {
		t.Fatalf("forge: %v", err)
	}
	if cfg.WriteConcurrencyLimit != 0 || cfg.WriteQueueLimit != 0 ||
		cfg.ReadConcurrencyLimit != 0 || cfg.ReadQueueLimit != 0 {
		t.Fatalf("admission defaults: got w=%d/%d r=%d/%d want all zero",
			cfg.WriteConcurrencyLimit, cfg.WriteQueueLimit,
			cfg.ReadConcurrencyLimit, cfg.ReadQueueLimit)
	}
}
