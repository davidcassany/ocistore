/*
Copyright © 2024 SUSE LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmd

import (
	"errors"

	"github.com/davidcassany/ocistore/pkg/logger"
	"github.com/spf13/cobra"
)

// umountCmd represents the umount command
var umountCmd = &cobra.Command{
	Use:     "umount MOUNTPOINT",
	Short:   "Unmounts the given mountpoint",
	Args:    cobra.ExactArgs(1),
	PreRunE: initCS,
	RunE: func(cmd *cobra.Command, args []string) error {
		flags := cmd.Flags()
		rmAllSnap, _ := flags.GetBool("remove-all")
		rmActiveSnap, _ := flags.GetBool("remove-active")
		key, _ := flags.GetString("snapshot-key")
		target := args[0]
		var removeSnap int

		if key == "" && (rmAllSnap || rmActiveSnap) {
			return errors.New("snapshot key is required to delete snapshots")
		}

		if rmAllSnap {
			removeSnap = -1
		} else if rmActiveSnap {
			removeSnap = 1
		}

		err := cs.Umount(target, key, removeSnap)
		if err != nil {
			return err
		}
		logger.Info("Target '%s' unmounted", target)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(umountCmd)

	umountCmd.Flags().String("snapshot-key", "", "The key of the snapshot to delete")
	umountCmd.Flags().Bool("remove-active", false, "Removes the unmounted active snapshot")
	umountCmd.Flags().Bool("remove-all", false, "Removes all snapshots under active too, stops walking the chain if the target snapshot has childs")
	umountCmd.MarkFlagsMutuallyExclusive("remove-active", "remove-all")
}
