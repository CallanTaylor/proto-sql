package protosql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/CallanTaylor/proto-sql/sqlproto"
	schema "github.com/jimsmart/schema"
)

func ExecuteTransaction(data interface{}, db *sql.DB, crudOperation sqlproto.CRUDOperation, tableName string) error {

	// Open a transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// Defer a function to either commit or rollback the transaction based on success or failure
	defer func() {
		if err != nil {
			fmt.Printf("ERROR: SQL transaction failed - rolling back: %v", err)
			// Rollback the transaction if an error occurred
			tx.Rollback()
			return
		}
		// Commit the transaction if all operations are successful
		err = tx.Commit()
		if err != nil {
			return
		}
	}()

	sqlStatement := ""

	switch crudOperation {
	case sqlproto.CRUDOperation_READ:
		sqlStatement, err = GenerateGetSQL(data, tableName)
	case sqlproto.CRUDOperation_UPDATE:
		sqlStatement, err = GenerateUpdateSQL(data, tableName)
	case sqlproto.CRUDOperation_CREATE:
		sqlStatement, err = GenerateInsertSQL(data, tableName)
	case sqlproto.CRUDOperation_DELETE:
		sqlStatement, err = GenerateDeleteSQL(data, tableName)
	}
	if err != nil {
		return fmt.Errorf("did not generate SQL statement : %v", err)
	}

	// Example: Insert data within a transaction
	row := tx.QueryRow(sqlStatement)

	// Get the type of the target struct
	targetType := reflect.TypeOf(data).Elem()

	// Get column names
	columns, err := getColumns(db, tableName)
	if err != nil {
		return err
	}

	// Create a slice of interface{} to hold the values
	values := make([]interface{}, len(columns))
	for i := range columns {
		values[i] = new(interface{})
	}

	// Scan values into the interface{} slice
	err = row.Scan(values...)
	if err != nil {
		return err
	}

	// Create a new instance of the target struct
	targetValue := reflect.New(targetType).Elem()

	// messageValue := reflect.ValueOf(data)
	messageType := reflect.TypeOf(data)
	// If the messageType is a pointer, dereference it
	if messageType.Kind() == reflect.Ptr {
		messageType = messageType.Elem()
	}

	fields := []string{}

	for i := 0; i < messageType.NumField(); i++ {

		fieldCheck := messageType.Field(i)
		fieldName := fieldCheck.Name

		exported := (fieldCheck.PkgPath == "")
		if !exported && !fieldCheck.Anonymous {
			continue
		}

		fields = append(fields, fieldName)
	}

	// Iterate over the fields of the struct and set values dynamically
	for i, v := range values {

		field := targetValue.FieldByName(fields[i])
		if field.IsValid() {
			// Use reflection to set the field value based on its type
			field.Set(reflect.ValueOf(*v.(*interface{})))
		}
	}

	// Set the values back to the original target
	reflect.ValueOf(data).Elem().Set(targetValue)

	return nil

}

// getColumns retrieves column names from a table in the database
func getColumns(db *sql.DB, tableName string) ([]string, error) {
	var columns []string

	// Query to get column names
	query := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = $1")

	// Query the database
	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Scan column names
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}

	return columns, nil
}

func GenerateGetSQL(data interface{}, tableName string) (string, error) {

	// Get the reflected type of the protobuf message
	messageType := reflect.TypeOf(data)
	// Get the value type of the protobuf message
	messageValue := reflect.ValueOf(data)

	// If the messageValue is a pointer, dereference it
	if messageValue.Kind() == reflect.Ptr {
		messageValue = messageValue.Elem()
	}

	// If the messageType is a pointer, dereference it
	if messageType.Kind() == reflect.Ptr {
		messageType = messageType.Elem()
	}

	// Ensure messageValue is struct
	if messageValue.Kind() != reflect.Struct {
		return "", fmt.Errorf("input is not a struct")
	}

	// Get UUID for user
	uuidValue := fmt.Sprintf("'%v'", messageValue.FieldByName("UUID").Interface())

	sql := "SELECT * FROM " + tableName + " WHERE UUID=" + uuidValue

	fmt.Println(sql)

	return sql, nil
}

// GenerateInsertSQL generates an Insert SQL statement for a given struct
func GenerateUpdateSQL(data interface{}, tableName string) (string, error) {

	// Get the reflected type of the protobuf message
	messageType := reflect.TypeOf(data)
	messageValue := reflect.ValueOf(data)

	// If the messageValue is a pointer, dereference it
	if messageValue.Kind() == reflect.Ptr {
		messageValue = messageValue.Elem()
	}

	// If the messageType is a pointer, dereference it
	if messageType.Kind() == reflect.Ptr {
		messageType = messageType.Elem()
	}

	// Ensure messageValue is struct
	if messageValue.Kind() != reflect.Struct {
		return "", fmt.Errorf("input is not a struct")
	}

	sql := fmt.Sprintf("UPDATE %s SET ", tableName)

	var columns []string

	uuidValue := ""

	// Loop through the fields of the message and generate column definitions
	for i := 0; i < messageType.NumField(); i++ {

		fieldCheck := messageType.Field(i)
		fieldName := fieldCheck.Name
		fieldType := messageValue.Type().Field(i)

		exported := (fieldType.PkgPath == "")
		if !exported && !fieldType.Anonymous {
			continue
		}

		if fieldName == "UUID" {
			uuidValue = fmt.Sprintf("'%v'", messageValue.Field(i).Interface())
			continue
		}

		if messageValue.Field(i).IsZero() {
			continue
		}

		field := messageValue.Type().Field(i)

		sql += field.Name + " = " + fmt.Sprintf("'%v'", messageValue.Field(i).Interface()) + ", "

		columns = append(columns, field.Name)
	}

	sql = strings.TrimSuffix(sql, ", ") + " WHERE UUID = " + uuidValue + " RETURNING *;"

	return sql, nil
}

// GenerateInsertSQL generates an Insert SQL statement for a given struct
func GenerateInsertSQL(data interface{}, tableName string) (string, error) {

	// Get the reflected type of the protobuf message
	messageType := reflect.TypeOf(data)
	messageValue := reflect.ValueOf(data)

	// If the messageValue is a pointer, dereference it
	if messageValue.Kind() == reflect.Ptr {
		messageValue = messageValue.Elem()
	}

	// If the messageType is a pointer, dereference it
	if messageType.Kind() == reflect.Ptr {
		messageType = messageType.Elem()
	}

	// Ensure messageValue is struct
	if messageValue.Kind() != reflect.Struct {
		return "", fmt.Errorf("input is not a struct")
	}

	var columns []string
	var values []string

	// Loop through the fields of the message and generate column definitions
	for i := 0; i < messageType.NumField(); i++ {

		fieldType := messageValue.Type().Field(i)

		exported := (fieldType.PkgPath == "")
		if !exported && !fieldType.Anonymous {
			continue
		}

		field := messageValue.Type().Field(i)
		columns = append(columns, field.Name)
		values = append(values, fmt.Sprintf("'%v'", messageValue.Field(i).Interface()))
	}

	columnsStr := strings.Join(columns, ", ")
	valuesStr := strings.Join(values, ", ")

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s;", tableName, columnsStr, valuesStr, columnsStr)
	fmt.Printf("SQL: %s", sql)
	return sql, nil
}

// GenerateInsertSQL generates an Insert SQL statement for a given struct
func GenerateDeleteSQL(data interface{}, tableName string) (string, error) {

	// Get the reflected type of the protobuf message
	messageType := reflect.TypeOf(data)
	messageValue := reflect.ValueOf(data)

	// If the messageValue is a pointer, dereference it
	if messageValue.Kind() == reflect.Ptr {
		messageValue = messageValue.Elem()
	}

	// If the messageType is a pointer, dereference it
	if messageType.Kind() == reflect.Ptr {
		messageType = messageType.Elem()
	}

	// Ensure messageValue is struct
	if messageValue.Kind() != reflect.Struct {
		return "", fmt.Errorf("input is not a struct")
	}

	// Get UUID for user
	uuidValue := fmt.Sprintf("'%v'", messageValue.FieldByName("UUID").Interface())

	return fmt.Sprintf("UPDATE %s SET deletedatnanos=%d WHERE UUID = "+uuidValue+" RETURNING *;", tableName, time.Now().UnixNano()), nil
}

func GenerateCreateTableSQL(data interface{}, tableName string) string {

	// Get the reflected type of the protobuf message
	messageType := reflect.TypeOf(data)
	messageValue := reflect.ValueOf(data)

	// If the messageValue is a pointer, dereference it
	if messageValue.Kind() == reflect.Ptr {
		messageValue = messageValue.Elem()
	}

	// SQL statement to create the table
	createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", tableName)

	// If the messageType is a pointer, dereference it
	if messageType.Kind() == reflect.Ptr {
		messageType = messageType.Elem()
	}

	// Loop through the fields of the message and generate column definitions
	for i := 0; i < messageType.NumField(); i++ {

		field := messageType.Field(i)
		fieldName := field.Name

		fieldType := messageValue.Type().Field(i)

		exported := (fieldType.PkgPath == "")
		if !exported && !fieldType.Anonymous {
			continue
		}

		// Determine the SQL type based on the field type
		sqlType := DetermineSQLType(field.Type)

		// Add the column definition to the SQL statement
		createTableSQL += fmt.Sprintf("%s %s, ", fieldName, sqlType)
	}

	// Remove the trailing comma and space
	createTableSQL = strings.TrimSuffix(createTableSQL, ", ")

	// Close the CREATE TABLE statement
	createTableSQL += ");"

	return createTableSQL
}

func GenerateUpdateShemaSQL(db *sql.DB, tableName string, data interface{}) (string, error) {

	// Fetch names of all tables
	tnames, err := schema.TableNames(db)
	if err != nil {
		return "", err
	}

	exists := map[string]bool{}

	// tnames is [][2]string
	for i := range tnames {
		if tnames[i][1] == tableName {

			// Fetch column metadata for given table
			tcols, err := schema.ColumnTypes(db, "", "users")
			if err != nil {
				return "", err
			}

			// tcols is []*sql.ColumnType
			for i := range tcols {
				exists[strings.ToLower(tcols[i].Name())] = true
			}
		}
	}

	// Get the reflected type of the protobuf message
	messageType := reflect.TypeOf(data)
	messageValue := reflect.ValueOf(data)

	// If the messageValue is a pointer, dereference it
	if messageValue.Kind() == reflect.Ptr {
		messageValue = messageValue.Elem()
	}

	// If the messageType is a pointer, dereference it
	if messageType.Kind() == reflect.Ptr {
		messageType = messageType.Elem()
	}

	type NewColumn struct {
		FieldName string
		FieldType string
	}

	newColumns := []NewColumn{}

	// Loop through the fields of the message and generate column definitions
	for i := 0; i < messageType.NumField(); i++ {

		field := messageType.Field(i)
		fieldName := field.Name

		fieldType := messageValue.Type().Field(i)

		exported := (fieldType.PkgPath == "")
		if !exported && !fieldType.Anonymous {
			continue
		}
		fieldNameLower := strings.ToLower(fieldName)

		if !exists[fieldNameLower] {
			newColumns = append(newColumns, NewColumn{
				FieldName: fieldNameLower,
				FieldType: DetermineSQLType(field.Type),
			})
		}
	}

	if len(newColumns) == 0 {
		fmt.Println("Shcema has not changed, no update required")
		return "", nil
	}

	sql := fmt.Sprintf("ALTER TABLE %s", tableName)

	for _, field := range newColumns {
		sql += fmt.Sprintf(" ADD COLUMN IF NOT EXISTS %s %s,", field.FieldName, field.FieldType)
	}

	sql = strings.TrimSuffix(sql, ",")

	sql += ";"

	fmt.Println(sql)

	return sql, nil

}

func DetermineSQLType(fieldType reflect.Type) string {
	// Determine the SQL type based on the Go type
	switch fieldType.Kind() {
	case reflect.Int64:
		return "BIGINT"
	case reflect.Int32:
		return "INT"
	case reflect.String:
		return "VARCHAR(255)"
	// Add more cases for other supported types
	default:
		return "TEXT"
	}
}

func UpdateTableSchema(tableName string, db *sql.DB, data interface{}) error {

	sql, err := GenerateUpdateShemaSQL(db, tableName, data)
	if err != nil {
		return err
	}

	if sql == "" {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// Defer a function to either commit or rollback the transaction based on success or failure
	defer func() {
		if err != nil {
			fmt.Printf("ERROR: SQL transaction failed - rolling back: %v", err)
			// Rollback the transaction if an error occurred
			tx.Rollback()
			return
		}
		// Commit the transaction if all operations are successful
		err = tx.Commit()
		if err != nil {
			return
		}
	}()

	// Example: Insert data within a transaction
	_, err = tx.Query(sql)
	if err != nil {
		return err
	}

	return nil
}

func CreateTableFromProto(db *sql.DB, data interface{}, messageName string) error {
	// Get the reflected type of the protobuf message
	messageType := reflect.TypeOf(data)
	messageValue := reflect.ValueOf(data)

	// If the messageValue is a pointer, dereference it
	if messageValue.Kind() == reflect.Ptr {
		messageValue = messageValue.Elem()
	}

	// SQL statement to create the table
	createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", messageName)

	// Loop through the fields of the message and generate column definitions
	for i := 0; i < messageType.NumField(); i++ {

		field := messageType.Field(i)
		fieldName := field.Name

		fieldType := messageValue.Type().Field(i)

		exported := (fieldType.PkgPath == "")
		if !exported && !fieldType.Anonymous {
			continue
		}

		// Determine the SQL type based on the field type
		sqlType := DetermineSQLType(field.Type)

		// Add the column definition to the SQL statement
		createTableSQL += fmt.Sprintf("%s %s, ", fieldName, sqlType)
	}

	// Remove the trailing comma and space
	createTableSQL = strings.TrimSuffix(createTableSQL, ", ")

	// Close the CREATE TABLE statement
	createTableSQL += ");"

	// Execute the SQL statement
	_, err := db.Exec(createTableSQL)
	return err
}

func TableExists(db *sql.DB, tableName string) (bool, error) {
	// Query the information schema to check if the table exists
	query := `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_name = $1
		);
	`

	var exists bool
	err := db.QueryRow(query, tableName).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}
