package commands

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/evrblk/monstera"
	"github.com/spf13/cobra"
)

// parseLogLevel maps the CLI's log level vocabulary onto slog.Level.
func parseLogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid log level %q (expected debug, info, warn, or error)", s)
	}
}

// logFlags holds the --log-level flag shared by every "grackle run" command.
// Every command writes one JSON stream to stdout — no format or
// destination flag — and tags each event with a "component" attribute
// naming where it was emitted from (see setupLogger/setupCoreLogging).
type logFlags struct {
	level string
}

func addLogFlags(cmd *cobra.Command, f *logFlags) {
	cmd.PersistentFlags().StringVar(&f.level, "log-level", "info", "Log level: debug, info, warn, or error")
}

// setupLogger parses f.level and returns a logger writing JSON to stdout,
// exiting the process on an invalid level.
func setupLogger(f logFlags) *slog.Logger {
	level, err := parseLogLevel(f.level)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}

// nodeCoreLogFlags controls the raft-backed `node` command's core
// diagnostic log (see monstera.NodeConfig's CoreLogPolicy and
// CoreLogDestination): a level, plus leader-only and replay filtering,
// which only make sense with real raft replicas. It writes JSON to stdout,
// tagged component=core, same as everything else in the process.
type nodeCoreLogFlags struct {
	level         string
	leaderOnly    bool
	includeReplay bool
}

func addNodeCoreLogFlags(cmd *cobra.Command, f *nodeCoreLogFlags) {
	cmd.PersistentFlags().StringVar(&f.level, "core-log-level", "info", "Core diagnostic log level: debug, info, warn, or error")
	cmd.PersistentFlags().BoolVar(&f.leaderOnly, "core-log-leader-only", true, "Keep only core diagnostic log lines produced while this replica was raft leader")
	cmd.PersistentFlags().BoolVar(&f.includeReplay, "core-log-include-replay", false, "Include restart/rejoin replay lines in the core diagnostic log")
}

// setupCoreLogging builds the CoreLogPolicy and destination handler to plug
// into monstera.NodeConfig, exiting the process on invalid input.
func setupCoreLogging(f nodeCoreLogFlags) (monstera.CoreLogPolicy, slog.Handler) {
	level, err := parseLogLevel(f.level)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}).
		WithAttrs([]slog.Attr{slog.String("component", "core")})

	return monstera.CoreLogPolicy{
		MinLevel:      level,
		LeaderOnly:    f.leaderOnly,
		IncludeReplay: f.includeReplay,
	}, handler
}
