// pouch — minimal client CLI for pouch (https://pouch.pointegrity.com).
//
// One subcommand matters: `pouch put` — pipe / file / clipboard /
// arg-vector to a pouch instance using an ingress key.
//
//   echo "quick note" | pouch put
//   pouch put README.md --label "project readme"
//   pouch put -c                              # from system clipboard
//   pbpaste | pouch put --label "from clipboard"
//   pouch put image.png --binary --mime image/png   # raw binary
//
// Auth: ingress key (POUCH_KEY). Get one from your pouch admin via
//
//   pouch key create --owner <you> --label <where-you'll-use-it>
//
// run on the pouch server. The plaintext key is shown ONCE; paste it
// into your config.
//
// The companion daemon for receiving drops (anchor) lives in this
// same repo as `pouch-anchor`. Same provisioning model, complementary
// direction.
package main

import (
	"fmt"
	"os"
)

// Version is the cli's reported version (and matches the daemon's
// version stamp in this repo, since they release together).
const Version = "0.3.1"

func main() {
	if len(os.Args) < 2 {
		// No subcommand — if stdin is piped, treat as `put` so
		// `cat foo | pouch` works as a one-liner.
		if isStdinPiped() {
			if err := runPut(nil); err != nil {
				fail("put", err)
			}
			return
		}
		printHelp()
		return
	}
	switch os.Args[1] {
	case "put":
		if err := runPut(os.Args[2:]); err != nil {
			fail("put", err)
		}
	case "init":
		if err := runInit(os.Args[2:]); err != nil {
			fail("init", err)
		}
	case "version", "--version", "-v":
		fmt.Println("pouch", Version)
	case "help", "--help", "-h":
		printHelp()
	default:
		fmt.Fprintf(os.Stderr, "pouch: unknown subcommand %q\n", os.Args[1])
		printHelp()
		os.Exit(2)
	}
}

func fail(cmd string, err error) {
	fmt.Fprintf(os.Stderr, "pouch %s: %v\n", cmd, err)
	os.Exit(1)
}

func printHelp() {
	fmt.Fprint(os.Stderr, "pouch — client for pouch (https://pouch.pointegrity.com).\n\n"+
		"Usage:\n"+
		"  pouch put [FILE|-]        send a drop. Body comes from FILE, stdin (- or\n"+
		"                            piped), or --clipboard. See 'pouch put --help'.\n"+
		"  pouch init                scaffold the OS-conventional config dir.\n"+
		"  pouch version             print version.\n"+
		"  pouch help                this help.\n\n"+
		"Config: ~/.config/pouch/config.env (Linux), ~/Library/Application Support/pouch/\n"+
		"        (macOS), %AppData%\\pouch\\ (Windows). Override with --config or\n"+
		"        $POUCH_CONFIG. Required values:\n\n"+
		"  POUCH_URL=https://pouch.pointegrity.com\n"+
		"  POUCH_KEY=pk_...                # ingress key from your pouch admin\n\n"+
		"To get an ingress key, ask whoever runs your pouch server to:\n\n"+
		"  pouch key create --owner <your-user-id> --label <a-name-you-pick>\n\n"+
		"The plaintext key is shown ONCE — save it to the config above.\n")
}

func isStdinPiped() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) == 0
}
