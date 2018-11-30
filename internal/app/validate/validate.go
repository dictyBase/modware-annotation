package validate

import (
	"fmt"

	"gopkg.in/urfave/cli.v1"
)

func ValidateServerArgs(c *cli.Context) error {
	for _, p := range []string{
		"arangodb-pass",
		"arangodb-database",
		"arangodb-user",
		"nats-host",
		"nats-port",
	} {
		if len(c.String(p)) == 0 {
			return cli.NewExitError(
				fmt.Sprintf("argument %s is missing", p),
				2,
			)
		}
	}
	return nil
}
