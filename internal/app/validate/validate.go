package validate

import (
	"fmt"

	"github.com/urfave/cli"
)

const errNo = 2

func ServerArgs(clt *cli.Context) error {
	for _, param := range []string{
		"arangodb-pass",
		"arangodb-database",
		"arangodb-user",
		"nats-host",
		"nats-port",
	} {
		if len(clt.String(param)) == 0 {
			return cli.NewExitError(
				fmt.Sprintf("argument %s is missing", param),
				errNo,
			)
		}
	}

	return nil
}
