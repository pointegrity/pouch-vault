// pouch-vault pair — first-boot subcommand that exchanges a
// one-time pairing key (minted by `pouch vault pair-key create` on
// the pouch server) for the long-lived credentials this vault uses
// for register / heartbeat / SSE. Pattern matches the
// "first-run wizard" approach: run once, print credentials, exit;
// user pastes them into vault.env, then runs the daemon normally.
//
// See decisions:
//   vault-pairing-three-renders-of-one-key
//   vault-host-architecture
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

func runPair(args []string) error {
	fs := flag.NewFlagSet("pair", flag.ExitOnError)
	var (
		pouchURL     string
		pairingKey   string
		machineLabel string
		publicURL    string
	)
	fs.StringVar(&pouchURL, "pouch-url", "", "pouch SaaS base URL (or POUCH_URL)")
	fs.StringVar(&pairingKey, "pairing-key", "", "one-time pairing key (or POUCH_PAIRING_KEY)")
	fs.StringVar(&machineLabel, "machine-label", "", "user-friendly host label (defaults to hostname)")
	fs.StringVar(&publicURL, "public-url", "", "push-mode public URL (rare; leave empty for pull mode)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if pouchURL == "" {
		pouchURL = os.Getenv("POUCH_URL")
	}
	if pairingKey == "" {
		pairingKey = os.Getenv("POUCH_PAIRING_KEY")
	}
	if pouchURL == "" || pairingKey == "" {
		return fmt.Errorf("--pouch-url and --pairing-key (or POUCH_URL + POUCH_PAIRING_KEY) required")
	}

	hostname, _ := os.Hostname()
	if machineLabel == "" {
		machineLabel = hostname
	}

	cli := NewPouchClient(strings.TrimRight(pouchURL, "/"), "")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := cli.Pair(ctx, PairInput{
		PairingKey:   pairingKey,
		Kind:         "local",
		MachineLabel: machineLabel,
		Hostname:     hostname,
		Version:      Version,
		PublicURL:    strings.TrimRight(publicURL, "/"),
	})
	if err != nil {
		return err
	}

	fmt.Println("Paired. Save the values below; they are shown ONCE.")
	fmt.Println()
	fmt.Printf("  vault id      = %s\n", res.VaultID)
	fmt.Printf("  vault name    = %s\n", res.VaultName)
	fmt.Printf("  channel id    = %s\n", res.ChannelID)
	fmt.Printf("  mode          = %s\n", res.Mode)
	fmt.Println()
	fmt.Println("Paste these into your vault.env:")
	fmt.Println()
	fmt.Printf("  POUCH_URL          %s\n", pouchURL)
	fmt.Printf("  POUCH_VAULT_KEY    %s\n", res.VaultKey)
	fmt.Printf("  POUCH_HMAC_SECRET  %s\n", res.HMACSecret)
	if publicURL != "" {
		fmt.Printf("  POUCH_PUBLIC_URL   %s\n", publicURL)
	}
	fmt.Println()
	fmt.Println("Then run `pouch-vault` (no arguments) to start the daemon.")
	return nil
}
