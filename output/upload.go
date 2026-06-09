package output

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/pganalyze/collector/state"
	"github.com/pganalyze/collector/util"
)

func SetupSnapshotUploadForAllServers(ctx context.Context, servers []*state.Server, opts state.CollectionOpts, logger *util.Logger) {
	if opts.ForceEmptyGrant {
		return
	}
	for _, server := range servers {
		go snapshotUploadForServer(ctx, server, logger.WithPrefixAndRememberErrors(server.Config.SectionName), opts.TestRun)
	}
}

func snapshotUploadForServer(ctx context.Context, server *state.Server, logger *util.Logger, testRun bool) {
	var compactLogTime time.Time
	compactLogStats := make(map[string]uint8)
	var failed bool
	var delay time.Duration

	for {
		if failed {
			// If the last snapshot submission failed, use an increasing backoff delay
			delay = min(5*delay, 10*time.Second)
		} else {
			// Default delay to avoid high CPU usage from looping continuously
			delay = 10 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		popCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
		tx, err := server.FullSnapshotQueue.Pop(popCtx)
		cancel()
		if err == nil {
			err = uploadViaWebsocketOrHttp(ctx, server, logger, testRun, tx.Snapshot)
			if err != nil {
				logger.PrintError("Error uploading full snapshot: %s", err)
				tx.Rollback()
				failed = true
			} else {
				tx.Commit()
				if !testRun {
					logger.PrintInfo("Submitted full snapshot successfully")
				}
				failed = false
			}
		}

		popCtx, cancel = context.WithTimeout(ctx, time.Millisecond)
		tx, err = server.CompactSnapshotQueue.Pop(popCtx)
		cancel()
		if err == nil {
			kind := tx.Kind
			err = uploadViaWebsocketOrHttp(ctx, server, logger, testRun, tx.Snapshot)
			if err != nil {
				logger.PrintError("Error uploading compact snapshot: %s", err)
				tx.Rollback()
				failed = true
			} else {
				tx.Commit()
				failed = false
			}
			if err != nil || testRun {
				continue
			}
			logger.PrintVerbose("Submitted compact %s snapshot successfully", kind)
			compactLogStats[kind] = compactLogStats[kind] + 1
			if compactLogTime.IsZero() {
				compactLogTime = time.Now().Truncate(time.Minute)
			} else if time.Since(compactLogTime) > time.Minute {
				details := summarizeCounts(compactLogStats)
				if len(details) > 0 {
					logger.PrintInfo("Submitted compact snapshots successfully: " + details)
				}
				compactLogTime = time.Now().Truncate(time.Minute)
				compactLogStats = make(map[string]uint8)
			}
		}
	}
}

func summarizeCounts(counts map[string]uint8) string {
	var keys []string
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	details := ""
	for i, kind := range keys {
		details += fmt.Sprintf("%d %s", counts[kind], kind)
		if i < len(keys)-1 {
			details += ", "
		}
	}
	return details
}

func uploadViaWebsocketOrHttp(ctx context.Context, server *state.Server, logger *util.Logger, testRun bool, data []byte) error {
	if server.WebSocket.Connected() {
		logger.PrintVerbose("Uploading snapshot to websocket")
		server.WebSocket.Write <- data
	} else if server.Config.APIRequireWebsocket {
		return errors.New("Error uploading snapshot: WebSocket not connected")
	} else {
		return uploadSnapshot(ctx, server.Config.HTTPClient, server.Grant.Load(), logger, data)
	}
	return nil
}
