package jobs

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"charm.land/log/v2"
	"github.com/charmbracelet/soft-serve/git"
	"github.com/charmbracelet/soft-serve/pkg/backend"
	"github.com/charmbracelet/soft-serve/pkg/config"
	"github.com/charmbracelet/soft-serve/pkg/sync"
)

func init() {
	Register("gcs-backup", gcsBackup{})
}

type gcsBackup struct{}

// Spec derives the spec used for gcs backup and implements Runner.
func (g gcsBackup) Spec(ctx context.Context) string {
	cfg := config.FromContext(ctx)
	if cfg.Jobs.GcsBackup != "" {
		return cfg.Jobs.GcsBackup
	}
	return "@weekly"
}

// Func runs the gcs backup job task and implements Runner.
func (g gcsBackup) Func(ctx context.Context) func() {
	cfg := config.FromContext(ctx)
	logger := log.FromContext(ctx).WithPrefix("jobs.gcs-backup")
	b := backend.FromContext(ctx)
	bundlesPath := filepath.Join(cfg.DataPath, "bundles")

	return func() {
		repos, err := b.Repositories(ctx)
		if err != nil {
			logger.Error("error getting repositories", "err", err)
			return
		}
		if len(repos) == 0 {
			logger.Info("no repositories to back up")
			return
		}

		logger.Info("creating git bundle gcs backups")
		// Divide the work up among the number of CPUs.
		wq := sync.NewWorkPool(ctx, runtime.GOMAXPROCS(0), sync.WithWorkPoolLogger(logger.Errorf))
		for _, repo := range repos {
			if repo.IsMirror() {
				continue
			}
			r, err := repo.Open()
			if err != nil {
				logger.Error("error opening repository", "repo", repo.Name(), "err", err)
				continue
			}

			wq.Add(repo.Name(), func() {
				cmd := []string{
					"-c", "pack.threads=1", // baecher.dev/stdout/reproducible-git-bundles
					"bundle", "create", "--progress", filepath.Join(bundlesPath, repo.Name()+".bundle"),
					"--all", // git-scm.com/docs/git-bundle#_specifying_references
				}
				cmdString := "git " + strings.Join(cmd, " ")
				logger.Info("running " + cmdString)
				gitCmd := git.NewCommand(cmd...).WithContext(ctx)
				if cmdOutput, err := gitCmd.RunInDir(r.Path); err != nil {
					logger.Error(
						"gitCmd",
						"cmdOutput", strings.TrimSpace(string(cmdOutput)),
						"err", strings.TrimSpace(err.Error()),
					)
				}
			})
		}
		wq.Run()

		cmd := []string{
			"gcloud", "storage", "rsync", "--checksums-only",
			bundlesPath, fmt.Sprintf("gs://%s", cfg.GcsBackup.Bucket),
		}
		cmdString := strings.Join(cmd, " ")
		logger.Info("running " + cmdString)
		gcsCmd := exec.CommandContext(ctx, cmd[0], cmd[1:]...)
		if cmdOutput, err := gcsCmd.CombinedOutput(); err != nil {
			logger.Error(
				"gcsCmd",
				"cmdOutput", strings.TrimSpace(string(cmdOutput)),
				"err", strings.TrimSpace(err.Error()),
			)
		}
	}
}
