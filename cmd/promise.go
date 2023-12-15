package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/promises"
	"github.com/spf13/cobra"
)

func newPromiseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "promise",
		Short: "Manage promises",
	}

	cmd.AddCommand(newCreatePromiseCommand())
	cmd.AddCommand(newDescribePromiseCommand())
	cmd.AddCommand(newListPromisesCommand())
	cmd.AddCommand(newCompletePromiseCommand())

	return cmd
}

func newCreatePromiseCommand() *cobra.Command {
	var id string
	var param promises.Value
	// var state promises.PromiseState
	var tags map[string]string
	var timeout int64
	var value promises.Value

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new promise",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}
			id = args[0]

			c := client.NewOrDie(API)

			body := promises.Promise{
				Id:    id,
				Param: &param,
				// State:   &state,
				Tags:    &tags,
				Timeout: timeout,
				Value:   &value,
			}

			var params *promises.CreatePromiseParams

			resp, err := c.PromisesV1Alpha1().CreatePromise(context.TODO(), params, body)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 && resp.StatusCode != 201 {
				bs, err := io.ReadAll(resp.Body)
				if err != nil {
					panic(err)
				}
				fmt.Printf("%s\n", string(bs))
				return
			}

			fmt.Printf("Created schedule: %s\n", id)
		},
	}

	cmd.Flags().Int64VarP(&timeout, "timeout", "t", 0, "Timeout in seconds")

	_ = cmd.MarkFlagRequired("timeout")

	return cmd
}

func newDescribePromiseCommand() *cobra.Command {
	var id string

	cmd := &cobra.Command{
		Use:   "describe [id]",
		Short: "Get details of a promise",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}
			id = args[0]

			c := client.NewOrDie(API)

			resp, err := c.PromisesV1Alpha1().GetPromise(context.TODO(), id)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer resp.Body.Close()

			bs, err := io.ReadAll(resp.Body)
			if err != nil {
				panic(err)
			}

			if resp.StatusCode != 200 {
				fmt.Printf("%s\n", string(bs))
				return
			}

			fmt.Printf("%s\n", string(bs))
		},
	}

	return cmd
}

func newListPromisesCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List promises",
		Run: func(cmd *cobra.Command, args []string) {
			c := client.NewOrDie(API)

			params := &promises.ListPromisesParams{
				Filters: &promises.QueryFilters{
					Id:     nil,
					State:  nil,
					Limit:  nil,
					Cursor: nil,
				},
			}

			resp, err := c.PromisesV1Alpha1().ListPromises(context.Background(), params)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer resp.Body.Close()

			bs, err := io.ReadAll(resp.Body)
			if err != nil {
				panic(err)
			}

			if resp.StatusCode != 200 {
				fmt.Printf("%s\n", string(bs))
				return
			}

			fmt.Printf("%s\n", string(bs))
		},
	}
}

func newCompletePromiseCommand() *cobra.Command {
	var id string
	var state string // make enum
	var value promises.Value

	cmd := &cobra.Command{
		Use:   "complete",
		Short: "Complete an existing promise",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("Error: must specify ID")
				return
			}
			id = args[0]

			c := client.NewOrDie(API)

			u := promises.PromiseStateComplete(state)
			body := promises.PromiseCompleteRequest{
				State: &u,
				Value: &value,
			}

			var params *promises.PatchPromisesIdParams

			resp, err := c.PromisesV1Alpha1().PatchPromisesId(context.TODO(), id, params, body)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 && resp.StatusCode != 201 {
				bs, err := io.ReadAll(resp.Body)
				if err != nil {
					panic(err)
				}
				fmt.Printf("%s\n", string(bs))
				return
			}

			fmt.Printf("Completed promise: %s\n", id)
		},
	}

	cmd.Flags().StringVar(&id, "id", "", "ID of the promise")
	cmd.Flags().StringVar(&state, "state", "", "State of the promise")

	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("state")

	return cmd
}
