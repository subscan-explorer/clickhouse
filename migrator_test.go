package clickhouse_test

import (
	"regexp"
	"testing"
	"time"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

type User struct {
	ID        uint64 `gorm:"primaryKey"`
	Name      string
	FirstName string
	LastName  string
	Age       int64 `gorm:"type:Nullable(Int64)"`
	Active    bool
	Salary    float32
	Attrs     map[string]string `gorm:"type:Map(String,String);"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

func TestAutoMigrate(t *testing.T) {
	type UserMigrateColumn struct {
		ID           uint64
		Name         string
		IsAdmin      bool
		Birthday     time.Time `gorm:"precision:4"`
		Debit        float64   `gorm:"precision:4"`
		Note         string    `gorm:"size:10;comment:my note"`
		DefaultValue string    `gorm:"default:hello world"`
	}

	if DB.Migrator().HasColumn("users", "is_admin") {
		t.Fatalf("users's is_admin column should not exists")
	}

	if err := DB.Table("users").AutoMigrate(&UserMigrateColumn{}); err != nil {
		t.Fatalf("no error should happen when auto migrate, but got %v", err)
	}

	if !DB.Migrator().HasTable("users") {
		t.Fatalf("users should exists")
	}

	if !DB.Migrator().HasColumn("users", "is_admin") {
		t.Fatalf("users's is_admin column should exists after auto migrate")
	}

	columnTypes, err := DB.Migrator().ColumnTypes("users")
	if err != nil {
		t.Fatalf("failed to get column types, got error %v", err)
	}

	for _, columnType := range columnTypes {
		switch columnType.Name() {
		case "id":
			if columnType.DatabaseTypeName() != "UInt64" {
				t.Fatalf("column id primary key should be correct, name: %v, column: %#v", columnType.Name(), columnType)
			}
		case "note":
			if length, ok := columnType.Length(); !ok || length != 10 {
				t.Fatalf("column name length should be correct, name: %v, column: %#v", columnType.Name(), columnType)
			}

			if comment, ok := columnType.Comment(); !ok || comment != "my note" {
				t.Fatalf("column name length should be correct, name: %v, column: %#v", columnType.Name(), columnType)
			}
		case "default_value":
			if defaultValue, ok := columnType.DefaultValue(); !ok || defaultValue != "hello world" {
				t.Fatalf("column name default_value should be correct, name: %v, column: %#v", columnType.Name(), columnType)
			}
		case "debit":
			if decimal, scale, ok := columnType.DecimalSize(); !ok || (scale != 0 || decimal != 4) {
				t.Fatalf("column name debit should be correct, name: %v, column: %#v", columnType.Name(), columnType)
			}
		case "birthday":
			if decimal, scale, ok := columnType.DecimalSize(); !ok || (scale != 0 || decimal != 4) {
				t.Fatalf("column name birthday should be correct, name: %v, column: %#v", columnType.Name(), columnType)
			}
		}
	}
}

func TestMigrator_HasIndex(t *testing.T) {
	type UserWithIndex struct {
		FirstName string    `gorm:"index:full_name"`
		LastName  string    `gorm:"index:full_name"`
		CreatedAt time.Time `gorm:"index"`
	}
	if DB.Migrator().HasIndex("users", "full_name") {
		t.Fatalf("users's full_name index should not exists")
	}

	if err := DB.Table("users").AutoMigrate(&UserWithIndex{}); err != nil {
		t.Fatalf("no error should happen when auto migrate, but got %v", err)
	}

	if !DB.Migrator().HasIndex("users", "full_name") {
		t.Fatalf("users's full_name index should exists after auto migrate")
	}

	if err := DB.Table("users").AutoMigrate(&UserWithIndex{}); err != nil {
		t.Fatalf("no error should happen when auto migrate again")
	}
}

func TestMigrator_DontSupportEmptyDefaultValue(t *testing.T) {
	options, err := clickhousego.ParseDSN(testDSN())
	if err != nil {
		t.Fatalf("Can not parse dsn, got error %v", err)
	}

	DB, err := gorm.Open(clickhouse.New(clickhouse.Config{
		Conn:                         clickhousego.OpenDB(options),
		DontSupportEmptyDefaultValue: true,
	}))
	if err != nil {
		t.Fatalf("failed to connect database, got error %v", err)
	}

	type MyTable struct {
		MyField string
	}

	// Create the table with AutoMigrate
	if err := DB.Table("mytable").AutoMigrate(&MyTable{}); err != nil {
		t.Fatalf("no error should happen when auto migrate, but got %v", err)
	}

	if err := DB.Table("mytable").AutoMigrate(&MyTable{}); err != nil {
		t.Fatalf("no error should happen when auto migrate, but got %v", err)
	}

	columnTypes, err := DB.Migrator().ColumnTypes("mytable")
	if err != nil {
		t.Fatalf("failed to inspect columns, got %v", err)
	}
	foundString := false
	for _, col := range columnTypes {
		if col.Name() == "my_field" {
			foundString = true
			if col.DatabaseTypeName() != "String" {
				t.Fatalf("my_field column should remain String, got %s", col.DatabaseTypeName())
			}
		}
	}
	if !foundString {
		t.Fatalf("my_field column not found after auto migrate")
	}
}

func TestColumnTypesNullableDoesNotPanic(t *testing.T) {
	columnTypes, err := DB.Migrator().ColumnTypes("users")
	if err != nil {
		t.Fatalf("failed to get column types, got error %v", err)
	}
	for _, columnType := range columnTypes {
		ct := columnType
		t.Run(ct.Name(), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Nullable panicked for column %s: %v", ct.Name(), r)
				}
			}()
			_, _ = ct.Nullable()
		})
	}
}

func TestMigrator_OnClusterSupport(t *testing.T) {
	type ClusterTable struct {
		ID        uint64
		Name      string
		CreatedAt time.Time
	}

	// Test ON CLUSTER extraction from gorm:table_options
	sqlStrings := make([]string, 0)

	// Create a new DB instance for this test
	options, err := clickhousego.ParseDSN(testDSN())
	if err != nil {
		t.Fatalf("Can not parse dsn, got error %v", err)
	}

	testDB, err := gorm.Open(clickhouse.New(clickhouse.Config{
		Conn: clickhousego.OpenDB(options),
	}))
	if err != nil {
		t.Fatalf("failed to connect database, got error %v", err)
	}

	// Replace raw callback to capture SQL
	if err := testDB.Callback().Raw().Replace("gorm:raw", func(db *gorm.DB) {
		sqlToExecute := db.Statement.SQL.String()
		sqlStrings = append(sqlStrings, sqlToExecute)
		// Don't actually execute for this test
	}); err != nil {
		t.Fatalf("no error should happen when registering a callback, but got %v", err)
	}

	// Test with ON CLUSTER in table_options
	err = testDB.Set("gorm:table_options", "ON CLUSTER 'test_cluster' ENGINE ReplicatedMergeTree ORDER BY id").Table("cluster_test").AutoMigrate(&ClusterTable{})
	if err != nil {
		t.Fatalf("no error should happen when auto migrate with ON CLUSTER, but got %v", err)
	}

	// Check if ON CLUSTER was placed correctly in the SQL
	if len(sqlStrings) == 0 {
		t.Fatalf("expected SQL to be captured")
	}

	createSQL := sqlStrings[len(sqlStrings)-1] // Get the last (CREATE TABLE) statement

	// Verify ON CLUSTER appears after table name but before column definitions
	expectedPattern := `CREATE TABLE.*cluster_test.* ON CLUSTER 'test_cluster' \(.*id.*\).*ENGINE ReplicatedMergeTree`
	matched, err := regexp.MatchString(expectedPattern, createSQL)
	if err != nil {
		t.Fatalf("regex error: %v", err)
	}
	if !matched {
		t.Fatalf("ON CLUSTER not placed correctly. Got SQL: %s", createSQL)
	}
}
