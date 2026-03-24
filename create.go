package clickhouse

import (
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
)

func (dialector *Dialector) Create(db *gorm.DB) {
	if db.Error == nil {
		if db.Statement.Schema != nil && !db.Statement.Unscoped {
			for _, c := range db.Statement.Schema.CreateClauses {
				db.Statement.AddClause(c)
			}
		}

		if db.Statement.SQL.String() == "" {
			db.Statement.SQL.Grow(180)
			db.Statement.AddClauseIfNotExists(clause.Insert{})

			if values := callbacks.ConvertToCreateValues(db.Statement); len(values.Values) >= 1 {
				db.Statement.AddClause(values)
				db.Statement.Build("INSERT", "VALUES", "ON CONFLICT")
			}
		}

		if !db.DryRun && db.Error == nil {
			_, err := db.Statement.ConnPool.ExecContext(db.Statement.Context, db.Statement.SQL.String(), db.Statement.Vars...)
			db.AddError(err)
		}
	}
}
