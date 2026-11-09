package meta

import "database/sql"

// sqlTx lets tests in this package call internal helpers inside s.tx.
type sqlTx = sql.Tx
