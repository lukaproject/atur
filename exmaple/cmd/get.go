package cmd

import (
	"context"
	"fmt"

	"github.com/lukaproject/atur/exmaple/db"
	"github.com/spf13/cobra"
)

var getCommand = &cobra.Command{
	Use:   "get",
	Short: "get user_info object",
	Run: func(*cobra.Command, []string) {
		ui := &UserInfo{}
		err := db.UserInfoTable.FindCtx(context.Background(), []byte(getUserId), ui)
		if err != nil {
			fmt.Printf("find error: %v\n", err)
			return
		}
		fmt.Printf("user_info=%s\n", string(ui.Serialize()))
	},
}

var (
	getUserId string
)

func initializeGet() {
	getCommand.Flags().StringVar(&getUserId, "userid", "user_id", "the user id of UserInfo which you want to get")
}
