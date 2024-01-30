package protosql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	schema "github.com/jimsmart/schema"
)

func GenerateGetSQL(data interface{}, tableName string) (string, error) {

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
			fmt.Printf("Fiel %s is nil\n", fieldName)
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

		// Skip non exported fields
		if strings.ToLower(fieldName[:1]) == fieldName[:1] {
			fmt.Printf("Field %s is not exported\n", fieldName)
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

		// Skip non exported fields
		if strings.ToLower(fieldName[:1]) == fieldName[:1] {
			// fmt.Printf("Field %s is not exported\n", fieldName)
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
