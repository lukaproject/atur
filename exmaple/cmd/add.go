package cmd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lukaproject/atur/exmaple/db"
	"github.com/spf13/cobra"
)

var addCommand = &cobra.Command{
	Use:   "add",
	Short: "Add a user info",
	Run:   addFunc,
}

var addUserInfo = &UserInfo{}

func initializeAdd() {
	addCommand.Flags().StringVar(&addUserInfo.UserId, "userid", "test_user_id", "the user_id of user_info you want to add")
	addCommand.Flags().StringVar(&addUserInfo.Name, "name", "test_name", "the name of user_info you want to add")
	addCommand.Flags().StringVar(&addUserInfo.Info, "info", "test_info", "the info of user_info you want to add")
}

func addFunc(*cobra.Command, []string) {
	b, err := json.Marshal(addUserInfo)
	if err != nil {
		fmt.Printf("error = %v\n", err)
		return
	}
	fmt.Printf("addUserInfo=%s\n", string(b))
	err = db.UserInfoTable.InsertCtx(context.Background(), addUserInfo)
	if err != nil {
		fmt.Printf("insert error = %v\n", err)
		return
	}
	fmt.Printf("Insert Success, user_id=%s\n", addUserInfo.UserId)
}
