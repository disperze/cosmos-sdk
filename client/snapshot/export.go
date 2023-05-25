package snapshot

import (
	"fmt"

	"github.com/cosmos/cosmos-sdk/server"
	servertypes "github.com/cosmos/cosmos-sdk/server/types"
	"github.com/cosmos/cosmos-sdk/store/iavl"
	"github.com/cosmos/cosmos-sdk/types"
	iavltree "github.com/cosmos/iavl"
	"github.com/spf13/cobra"
)

// ExportSnapshotCmd returns a command to take a snapshot of the application state
func ExportSnapshotCmd(appCreator servertypes.AppCreator) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export app state to snapshot store",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := server.GetServerContextFromCmd(cmd)
			height, err := cmd.Flags().GetInt64("height")
			if err != nil {
				return err
			}

			home := ctx.Config.RootDir
			db, err := openDB(home, server.GetAppDBBackend(ctx.Viper))
			if err != nil {
				return err
			}

			defer db.Close()
			app := appCreator(ctx.Logger, db, nil, ctx.Viper)

			if height == 0 {
				height = app.CommitMultiStore().LastCommitID().Version
			}

			keyName, _ := cmd.Flags().GetString("key")
			if keyName != "" {
				store := app.CommitMultiStore().GetCommitKVStore(types.NewKVStoreKey(keyName))
				kvStore, ok := store.(*iavl.Store)
				if !ok {
					return fmt.Errorf("store %s is not an iavl store", keyName)
				}

				exporter, err := kvStore.Export(height)
				if err != nil {
					return err
				}
				defer exporter.Close()

				total := 0
				for {
					_, err := exporter.Next()
					if err == iavltree.ExportDone {
						break
					} else if err != nil {
						return err
					}

					total += 1
				}

				ctx.Logger.Debug(fmt.Sprintf("Exported %d nodes", total))
				return nil
			}
			ctx.Logger.Debug("Exporting snapshot", "height", height)
			fmt.Printf("Exporting snapshot for height %d\n", height)

			sm := app.SnapshotManager()
			snapshot, err := sm.Create(uint64(height))
			if err != nil {
				return err
			}

			ctx.Logger.Info("Snapshot created at height %d, format %d, chunks %d\n", snapshot.Height, snapshot.Format, snapshot.Chunks)
			fmt.Printf("Snapshot created at height %d, format %d, chunks %d\n", snapshot.Height, snapshot.Format, snapshot.Chunks)
			return nil
		},
	}

	cmd.Flags().Int64("height", 0, "Height to export, default to latest state height")
	cmd.Flags().String("key", "", "StoreKey")

	return cmd
}
