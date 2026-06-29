package app

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"github.com/escoffier-labs/miseledger/internal/adapter"
	"github.com/escoffier-labs/miseledger/internal/ingest"
)

// cmdCrawlDiscord ingests the local discrawl Discord archive by shelling out to
// `discrawl export adapter` (which emits miseledger.adapter.v1 JSONL) and
// streaming its output straight into the adapter importer. It mirrors
// cmdImportSourceHarvest but talks to the discrawl binary, which owns the
// Discord SQLite schema, so MiseLedger never duplicates that schema knowledge.
//
// Pass-through flags (--since, --limit, --channel, --guild, --guilds) go to
// discrawl unchanged; --json and --dry-run are handled here.
func cmdCrawlDiscord(args []string, out, errw io.Writer) int {
	if hasBoolFlag(args, "help") || hasBoolFlag(args, "h") {
		fmt.Fprintln(out, "usage: miseledger crawl discord [--since RFC3339] [--limit N] [--channel NAME] [--guild ID] [--json] [--dry-run]")
		return 0
	}
	asJSON, dryRun, passArgs := splitWrapperFlags(args)
	exportArgs := append([]string{"export", "adapter", "--out", "-"}, passArgs...)

	if dryRun {
		records, warnings, err := dryRunDiscrawl(exportArgs)
		if err != nil {
			return fatalf(errw, "crawl discord: %s", err)
		}
		if asJSON {
			writeJSON(out, map[string]any{"dry_run": true, "generated_records": records, "warnings": warnings})
		} else {
			fmt.Fprintf(out, "generated=%d warnings=%d\n", records, len(warnings))
		}
		return 0
	}

	db, _, err := openMigrated()
	if err != nil {
		return fatalf(errw, "crawl discord: %s", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), externalScannerTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "discrawl", exportArgs...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fatalf(errw, "crawl discord: %s", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return fatalf(errw, "crawl discord: %s", err)
	}
	result, importErr := ingest.ImportAdapterReader(db, stdout, "discrawl://export/adapter", "discord")
	waitErr := cmd.Wait()
	if ctx.Err() == context.DeadlineExceeded {
		return fatalf(errw, "crawl discord: timed out after %s", externalScannerTimeout)
	}
	if importErr != nil {
		return fatalf(errw, "crawl discord: %s", importErr)
	}
	if waitErr != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = waitErr.Error()
		}
		return fatalf(errw, "crawl discord: %s", msg)
	}
	if asJSON {
		writeJSON(out, result)
	} else {
		fmt.Fprintf(out, "imported=%d warnings=%d already_known=%v source=%s\n", result.Inserted, len(result.Warnings), result.AlreadyKnown, result.SourceKind)
	}
	return 0
}

// dryRunDiscrawl runs the export and counts valid adapter records without
// touching the database, so `crawl discord --dry-run` is a safe preview.
func dryRunDiscrawl(exportArgs []string) (int, []string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), externalScannerTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "discrawl", exportArgs...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, nil, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return 0, nil, err
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	records := 0
	warnings := []string{}
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if _, err := adapter.Parse(line); err != nil {
			warnings = append(warnings, err.Error())
			continue
		}
		records++
	}
	if err := scanner.Err(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return records, warnings, fmt.Errorf("discrawl timed out after %s", externalScannerTimeout)
		}
		return 0, nil, err
	}
	if err := cmd.Wait(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return records, warnings, fmt.Errorf("discrawl timed out after %s", externalScannerTimeout)
		}
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return 0, nil, fmt.Errorf("%s", msg)
	}
	return records, warnings, nil
}
