package repository

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	dtoMapper "github.com/dranikpg/dto-mapper"
	"github.com/go-playground/validator/v10"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// Todo: use gorm smart select, then no need for mapping

var (
	Connected     bool = false
	dbMap         map[string]*gorm.DB
	dbMutex       sync.RWMutex
	defaultSchema string
)

func init() {
	dbMap = make(map[string]*gorm.DB)
}

// Connect establishes connections to multiple schemas in the same PostgreSQL database
func Connect(sqlHost, sqlPort, sqlDbName, sqlSslmode, sqlUser, sqlPassword string, schemas []string) error {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	if len(schemas) == 0 {
		return errors.New("at least one schema must be provided")
	}

	for _, currentSchema := range schemas {
		sqlDsn := fmt.Sprintf("host=%s port=%s dbname=%s sslmode=%s user=%s password=%s",
			sqlHost, sqlPort, sqlDbName, sqlSslmode, sqlUser, sqlPassword)

		database, err := gorm.Open(postgres.New(postgres.Config{
			DSN: sqlDsn,
		}), &gorm.Config{
			NamingStrategy: schema.NamingStrategy{
				TablePrefix:   currentSchema + ".", // schema name
				SingularTable: true,                // use singular table name
			},
		})

		if err != nil {
			return fmt.Errorf("failed to connect to database for schema %s: %w", currentSchema, err)
		}

		// Add uuid-ossp extension for postgres database
		database.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")

		dbMap[currentSchema] = database
	}

	// Set default schema as the first one provided
	defaultSchema = schemas[0]
	Connected = true

	// Todo: set connection pool for each database connection
	/*
		for _, db := range dbMap {
			sqlDB, err := db.DB()
			if err != nil {
				return err
			}
			sqlDB.SetMaxIdleConns(10)
			sqlDB.SetMaxOpenConns(100)
			sqlDB.SetConnMaxLifetime(time.Hour)
		}
	*/
	// Todo: optimize performance https://gorm.io/docs/performance.html
	return nil
}

// Migrate runs AutoMigrate for the specified schema
func Migrate(schemaName string, models ...interface{}) error {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	if !Connected {
		return errors.New("database not connected")
	}

	db, exists := dbMap[schemaName]
	if !exists {
		return fmt.Errorf("schema %s not connected", schemaName)
	}

	err := db.AutoMigrate(models...)
	return err
}

// Ping checks the connection for the specified schema
func Ping(schemaName string) error {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	if !Connected {
		return errors.New("not connected")
	}

	db, exists := dbMap[schemaName]
	if !exists {
		return fmt.Errorf("schema %s not connected", schemaName)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	err = sqlDB.Ping()
	if err != nil {
		Connected = false
		return err
	}
	return nil
}

// Close closes all database connections
func Close() error {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	if !Connected {
		return errors.New("not connected")
	}

	for schemaName, db := range dbMap {
		sqlDB, err := db.DB()
		if err != nil {
			return fmt.Errorf("failed to get sql.DB for schema %s: %w", schemaName, err)
		}
		if err := sqlDB.Close(); err != nil {
			return fmt.Errorf("failed to close connection for schema %s: %w", schemaName, err)
		}
	}
	dbMap = make(map[string]*gorm.DB)
	Connected = false
	defaultSchema = ""
	return nil
}

// Stats returns database statistics for the specified schema
func Stats(schemaName string) (stats sql.DBStats, err error) {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	if !Connected {
		return stats, errors.New("not connected")
	}

	db, exists := dbMap[schemaName]
	if !exists {
		return stats, fmt.Errorf("schema %s not connected", schemaName)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return stats, err
	}
	return sqlDB.Stats(), nil
}

// SQLQuery represents a query with schema support
type SQLQuery[M any, E any] struct {
	expressStr string
	args       []interface{}
	db         *gorm.DB
	schema     string
}

// NewQuery creates a new query instance for the specified schema
func NewQuery[M any, E any](schemaName string, dbInstances ...interface{}) *SQLQuery[M, E] {
	query := &SQLQuery[M, E]{schema: schemaName}

	var isDBInitialized bool = false
	for _, db := range dbInstances {
		if db != nil {
			switch t := db.(type) {
			case *gorm.DB:
				if t != nil {
					query.db = t
					isDBInitialized = true
				}
			default:
			}
		}
	}

	// Assign default DB for the schema
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	if !isDBInitialized {
		if schemaName == "" {
			schemaName = defaultSchema
		}
		db, exists := dbMap[schemaName]
		if !exists {
			panic(fmt.Sprintf("schema %s not initialized", schemaName))
		}
		query.db = db
		isDBInitialized = true
	}

	if !isDBInitialized {
		panic("database is not initialized")
	}

	query.expressStr = ""
	query.args = make([]interface{}, 0)
	return query
}

// AddConditionOfTextField adds a filter condition for a text field
func (query *SQLQuery[M, E]) AddConditionOfTextField(cascadingLogic string, fieldName string, comparisonOperator string, value interface{}) {
	if fieldName == "" {
		return
	}

	if query.expressStr == "" {
		if comparisonOperator == "LIKE" {
			query.expressStr = fmt.Sprintf("lower(\"%s\") %s ?", fieldName, comparisonOperator)
		} else {
			query.expressStr = fmt.Sprintf("\"%s\" %s ?", fieldName, comparisonOperator)
		}
	} else {
		if comparisonOperator == "LIKE" {
			query.expressStr = fmt.Sprintf("%s %s lower(\"%s\") %s ?", query.expressStr, cascadingLogic, fieldName, comparisonOperator)
		} else {
			query.expressStr = fmt.Sprintf("%s %s \"%s\" %s ?", query.expressStr, cascadingLogic, fieldName, comparisonOperator)
		}
	}

	if comparisonOperator == "LIKE" {
		s, ok := value.(string)
		if ok {
			s = strings.ToLower(s)
			query.args = append(query.args, "%"+s+"%")
		}
	} else {
		query.args = append(query.args, value)
	}
}

// AddTwoConditionOfTextField adds two filter conditions for text fields
func (query *SQLQuery[M, E]) AddTwoConditionOfTextField(cascadingLogic string, fieldName1 string, comparisonOperator1 string, value1 interface{}, combineLogic string, fieldName2 string, comparisonOperator2 string, value2 interface{}) {
	if fieldName1 == "" || fieldName2 == "" {
		return
	}

	if query.expressStr == "" {
		if comparisonOperator1 == "LIKE" && comparisonOperator2 != "LIKE" {
			query.expressStr = fmt.Sprintf("lower(\"%s\") %s ? %s \"%s\" %s ?", fieldName1, comparisonOperator1, combineLogic, fieldName2, comparisonOperator2)
		} else if comparisonOperator1 != "LIKE" && comparisonOperator2 == "LIKE" {
			query.expressStr = fmt.Sprintf("\"%s\" %s ? %s lower(\"%s\") %s ?", fieldName1, comparisonOperator1, combineLogic, fieldName2, comparisonOperator2)
		} else if comparisonOperator1 == "LIKE" && comparisonOperator2 == "LIKE" {
			query.expressStr = fmt.Sprintf("lower(\"%s\") %s ? %s lower(\"%s\") %s ?", fieldName1, comparisonOperator1, combineLogic, fieldName2, comparisonOperator2)
		} else {
			query.expressStr = fmt.Sprintf("\"%s\" %s ? %s \"%s\" %s ?", fieldName1, comparisonOperator1, combineLogic, fieldName2, comparisonOperator2)
		}
	} else {
		if comparisonOperator1 == "LIKE" && comparisonOperator2 != "LIKE" {
			query.expressStr = fmt.Sprintf("%s %s lower(\"%s\") %s ? %s \"%s\" %s ?", query.expressStr, cascadingLogic, fieldName1, comparisonOperator1, combineLogic, fieldName2, comparisonOperator2)
		} else if comparisonOperator1 != "LIKE" && comparisonOperator2 == "LIKE" {
			query.expressStr = fmt.Sprintf("%s %s \"%s\" %s ? %s lower(\"%s\") %s ?", query.expressStr, cascadingLogic, fieldName1, comparisonOperator1, combineLogic, fieldName2, comparisonOperator2)
		} else if comparisonOperator1 == "LIKE" && comparisonOperator2 == "LIKE" {
			query.expressStr = fmt.Sprintf("%s %s lower(\"%s\") %s ? %s lower(\"%s\") %s ?", query.expressStr, cascadingLogic, fieldName1, comparisonOperator1, combineLogic, fieldName2, comparisonOperator2)
		} else {
			query.expressStr = fmt.Sprintf("%s %s \"%s\" %s ? %s \"%s\" %s ?", query.expressStr, cascadingLogic, fieldName1, comparisonOperator1, combineLogic, fieldName2, comparisonOperator2)
		}
	}

	if comparisonOperator1 == "LIKE" {
		s, ok := value1.(string)
		if ok {
			s = strings.ToLower(s)
			query.args = append(query.args, "%"+s+"%")
		}
	} else {
		query.args = append(query.args, value1)
	}

	if comparisonOperator2 == "LIKE" {
		s, ok := value2.(string)
		if ok {
			s = strings.ToLower(s)
			query.args = append(query.args, "%"+s+"%")
		}
	} else {
		query.args = append(query.args, value2)
	}
}

// AddConditionOfJsonbField adds a filter condition for a JSONB field
func (query *SQLQuery[M, E]) AddConditionOfJsonbField(cascadingLogic string, fieldName string, key string, comparisonOperator string, value interface{}) {
	if fieldName == "" {
		return
	}

	if query.expressStr == "" {
		if comparisonOperator == "LIKE" {
			query.expressStr = fmt.Sprintf("lower(\"%s\" ->> '%s') %s ?", fieldName, key, comparisonOperator)
		} else {
			query.expressStr = fmt.Sprintf("\"%s\" ->> '%s' %s ?", fieldName, key, comparisonOperator)
		}
	} else {
		if comparisonOperator == "LIKE" {
			query.expressStr = fmt.Sprintf("%s %s lower(\"%s\" ->> '%s') %s ?", query.expressStr, cascadingLogic, fieldName, key, comparisonOperator)
		} else {
			query.expressStr = fmt.Sprintf("%s %s \"%s\" ->> '%s' %s ?", query.expressStr, cascadingLogic, fieldName, key, comparisonOperator)
		}
	}

	if comparisonOperator == "LIKE" {
		s, ok := value.(string)
		if ok {
			s = strings.ToLower(s)
			query.args = append(query.args, "%"+s+"%")
		}
	} else {
		query.args = append(query.args, value)
	}
}

// ExecNoPaging executes the query without pagination
func (query *SQLQuery[M, E]) ExecNoPaging(sort string) (dtos []M, count int64, err error) {
	if !Connected {
		return dtos, 0, errors.New("database not connected")
	}
	count = 0

	if strings.HasPrefix(sort, "-") {
		sort = "\"" + strings.TrimPrefix(sort, "-") + "\"" + " desc"
	} else if strings.HasPrefix(sort, "+") {
		sort = "\"" + strings.TrimPrefix(sort, "+") + "\"" + " asc"
	} else {
		sort = "\"created_at\"" + " desc"
	}

	var items []E
	result := query.db.Order(sort).Where(query.expressStr, query.args...).Find(&items)
	if result.Error != nil {
		return dtos, count, result.Error
	}

	dtos = make([]M, 0)
	for _, item := range items {
		var dto M
		if err := dtoMapper.Map(&dto, item); err != nil {
			return dtos, count, err
		}
		dtos = append(dtos, dto)
		count++
	}

	return dtos, count, result.Error
}

// ExecWithPaging executes the query with pagination
func (query *SQLQuery[M, E]) ExecWithPaging(sort string, limit int, page int) (dtos []M, count int64, err error) {
	if !Connected {
		return dtos, 0, errors.New("database not connected")
	}

	if limit < 1 {
		limit = 100
	}
	if page < 1 {
		page = 1
	}
	if strings.HasPrefix(sort, "-") {
		sort = "\"" + strings.TrimPrefix(sort, "-") + "\"" + " desc"
	} else if strings.HasPrefix(sort, "+") {
		sort = "\"" + strings.TrimPrefix(sort, "+") + "\"" + " asc"
	} else {
		sort = "\"created_at\"" + " desc"
	}

	offset := limit * (page - 1)

	var entityModel E
	result := query.db.Model(entityModel).Where(query.expressStr, query.args...).Count(&count)
	if result.Error != nil {
		return dtos, 0, result.Error
	}

	var items []E
	result = query.db.Limit(limit).Offset(offset).Order(sort).Where(query.expressStr, query.args...).Find(&items)
	if result.Error != nil {
		return dtos, count, result.Error
	}

	dtos = make([]M, 0)
	for _, item := range items {
		var dto M
		if err := dtoMapper.Map(&dto, item); err != nil {
			return dtos, count, err
		}
		dtos = append(dtos, dto)
	}

	return dtos, count, result.Error
}

// CreateItemFromDTO creates a new item in the specified schema
func CreateItemFromDTO[M any, E any](schemaName string, dto M) (M, error) {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	if !Connected {
		return dto, errors.New("database not connected")
	}

	if schemaName == "" {
		schemaName = defaultSchema
	}
	db, exists := dbMap[schemaName]
	if !exists {
		return dto, fmt.Errorf("schema %s not connected", schemaName)
	}

	validate := validator.New()
	err := validate.Struct(dto)
	if err != nil {
		return dto, err
	}

	var item E
	if err := dtoMapper.Map(&item, dto); err != nil {
		return dto, err
	}

	var entity E
	if result := db.Model(entity).Create(&item); result.Error != nil {
		return dto, result.Error
	}

	if err := dtoMapper.Map(&dto, item); err != nil {
		return dto, err
	}
	return dto, nil
}

// ReadItemByIDIntoDTO reads an item by ID from the specified schema
func ReadItemByIDIntoDTO[M any, E any](schemaName string, id string) (dto M, err error) {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	if !Connected {
		return dto, errors.New("database not connected")
	}

	if schemaName == "" {
		schemaName = defaultSchema
	}
	db, exists := dbMap[schemaName]
	if !exists {
		return dto, fmt.Errorf("schema %s not connected", schemaName)
	}

	var item E
	if err := db.Where("id = ?", id).First(&item).Error; err != nil {
		return dto, err
	}

	if err := dtoMapper.Map(&dto, item); err != nil {
		return dto, err
	}
	return dto, nil
}

// ReadMultiItemsByIDIntoDTO reads multiple items by IDs from the specified schema
func ReadMultiItemsByIDIntoDTO[M any, E any](schemaName string, ids []string, sort string) (dtos []M, count int64, err error) {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	if !Connected {
		return dtos, 0, errors.New("database not connected")
	}

	if schemaName == "" {
		schemaName = defaultSchema
	}
	db, exists := dbMap[schemaName]
	if !exists {
		return dtos, 0, fmt.Errorf("schema %s not connected", schemaName)
	}

	count = 0
	if strings.HasPrefix(sort, "-") {
		sort = "\"" + strings.TrimPrefix(sort, "-") + "\"" + " desc"
	} else if strings.HasPrefix(sort, "+") {
		sort = "\"" + strings.TrimPrefix(sort, "+") + "\"" + " asc"
	} else {
		sort = "\"created_at\"" + " desc"
	}

	var items []E
	result := db.Order(sort).Where("id IN ?", ids).Find(&items)
	if result.Error != nil {
		return dtos, 0, result.Error
	}

	dtos = make([]M, 0)
	for _, item := range items {
		var dto M
		if err := dtoMapper.Map(&dto, item); err != nil {
			return dtos, count, err
		}
		dtos = append(dtos, dto)
		count++
	}

	return dtos, count, nil
}

// ReadAllItemsIntoDTO reads all items from the specified schema
func ReadAllItemsIntoDTO[M any, E any](schemaName string, sort string) (dtos []M, count int64, err error) {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	if !Connected {
		return dtos, 0, errors.New("database not connected")
	}

	if schemaName == "" {
		schemaName = defaultSchema
	}
	db, exists := dbMap[schemaName]
	if !exists {
		return dtos, 0, fmt.Errorf("schema %s not connected", schemaName)
	}

	count = 0
	if strings.HasPrefix(sort, "-") {
		sort = "\"" + strings.TrimPrefix(sort, "-") + "\"" + " desc"
	} else if strings.HasPrefix(sort, "+") {
		sort = "\"" + strings.TrimPrefix(sort, "+") + "\"" + " asc"
	} else {
		sort = "\"created_at\"" + " desc"
	}

	var items []E
	result := db.Order(sort).Find(&items)
	if result.Error != nil {
		return dtos, 0, result.Error
	}

	dtos = make([]M, 0)
	for _, item := range items {
		var dto M
		if err := dtoMapper.Map(&dto, item); err != nil {
			return dtos, count, err
		}
		dtos = append(dtos, dto)
		count++
	}

	return dtos, count, nil
}

// ReadItemWithFilterIntoDTO reads an item with a filter from the specified schema
func ReadItemWithFilterIntoDTO[M any, E any](schemaName string, query string, args ...interface{}) (dto M, err error) {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	if !Connected {
		return dto, errors.New("database not connected")
	}

	if schemaName == "" {
		schemaName = defaultSchema
	}
	db, exists := dbMap[schemaName]
	if !exists {
		return dto, fmt.Errorf("schema %s not connected", schemaName)
	}

	var item E
	result := db.Where(query, args...).First(&item)
	if result.Error != nil {
		return dto, result.Error
	}

	if err := dtoMapper.Map(&dto, item); err != nil {
		return dto, err
	}

	return dto, nil
}

// UpdateItemByIDFromDTO updates an item by ID in the specified schema
func UpdateItemByIDFromDTO[M any, E any](schemaName string, id string, dto M) (M, error) {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	if !Connected {
		return dto, errors.New("database not connected")
	}

	if schemaName == "" {
		schemaName = defaultSchema
	}
	db, exists := dbMap[schemaName]
	if !exists {
		return dto, fmt.Errorf("schema %s not connected", schemaName)
	}

	var item E
	if err := db.Where("id = ?", id).First(&item).Error; err != nil {
		return dto, err
	}

	if err := dtoMapper.Map(&item, dto); err != nil {
		return dto, err
	}

	if err := db.Model(item).Where("id = ?", id).Updates(&item).Error; err != nil {
		return dto, err
	}

	if err := dtoMapper.Map(&dto, item); err != nil {
		return dto, err
	}

	return dto, nil
}

// DeleteItemByID deletes an item by ID in the specified schema
func DeleteItemByID[E any](schemaName string, id string) (err error) {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	if !Connected {
		return errors.New("database not connected")
	}

	if schemaName == "" {
		schemaName = defaultSchema
	}
	db, exists := dbMap[schemaName]
	if !exists {
		return fmt.Errorf("schema %s not connected", schemaName)
	}

	var item E
	if err = db.Where("id = ?", id).Delete(&item).Error; err != nil {
		return err
	}

	return nil
}

// DeleteAllItem deletes all items in the specified schema
func DeleteAllItem[E any](schemaName string, softDelete bool) (err error) {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	if !Connected {
		return errors.New("database not connected")
	}

	if schemaName == "" {
		schemaName = defaultSchema
	}
	db, exists := dbMap[schemaName]
	if !exists {
		return fmt.Errorf("schema %s not connected", schemaName)
	}

	var item E
	if softDelete {
		if err = db.Where("created_at > ?", "2000-01-01 00:00:00").Delete(&item).Error; err != nil {
			return err
		}
	} else {
		if err = db.Unscoped().Where("created_at > ?", "2000-01-01 00:00:00").Delete(&item).Error; err != nil {
			return err
		}
	}

	return nil
}

// CheckItemExistedByID checks if an item exists by ID in the specified schema
func CheckItemExistedByID[E any](schemaName string, id string) (exists bool, err error) {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	if !Connected {
		return exists, errors.New("database not connected")
	}

	if schemaName == "" {
		schemaName = defaultSchema
	}
	db, exists := dbMap[schemaName]
	if !exists {
		return exists, fmt.Errorf("schema %s not connected", schemaName)
	}

	var item E
	if err = db.Model(item).Select("count(*) > 0").Where("id = ?", id).Find(&exists).Error; err != nil {
		return exists, err
	}

	return exists, nil
}

// UpdateSingleColumn updates a single column for an item in the specified schema
func UpdateSingleColumn[E any](schemaName string, id string, columnName string, value interface{}) error {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	if !Connected {
		return errors.New("database not connected")
	}

	if schemaName == "" {
		schemaName = defaultSchema
	}
	db, exists := dbMap[schemaName]
	if !exists {
		return fmt.Errorf("schema %s not connected", schemaName)
	}

	var item E
	if err := db.Where("id = ?", id).First(&item).Error; err != nil {
		return err
	}

	if err := db.Model(item).Where("id = ?", id).Update(columnName, value).Error; err != nil {
		return err
	}

	return nil
}

// AddJoin adds a JOIN clause to the query
func (query *SQLQuery[M, E]) AddJoin(joinType, table, condition string) *SQLQuery[M, E] {
	query.db = query.db.Joins(fmt.Sprintf("%s %s ON %s", joinType, table, condition))
	return query
}

// AddPreload adds a Preload clause to eagerly load related data
func (query *SQLQuery[M, E]) AddPreload(relation string) *SQLQuery[M, E] {
	query.db = query.db.Preload(relation)
	return query
}

// ExecCustomQuery executes a custom SQL query
func (query *SQLQuery[M, E]) ExecCustomQuery(rawQuery string, args ...interface{}) (dtos []M, count int64, err error) {
	if !Connected {
		return dtos, 0, errors.New("database not connected")
	}
	count = 0

	var items []E
	result := query.db.Raw(rawQuery, args...).Scan(&items)
	if result.Error != nil {
		return dtos, 0, result.Error
	}

	dtos = make([]M, 0)
	for _, item := range items {
		var dto M
		if err := dtoMapper.Map(&dto, item); err != nil {
			return dtos, count, err
		}
		dtos = append(dtos, dto)
		count++
	}
	return dtos, count, nil
}

// ExecCustomQueryWithPaging executes a custom SQL query with pagination
func (query *SQLQuery[M, E]) ExecCustomQueryWithPaging(rawQuery string, limit, page int, args ...interface{}) (dtos []M, count int64, err error) {
	if !Connected {
		return dtos, 0, errors.New("database not connected")
	}

	if limit < 1 {
		limit = 100
	}
	if page < 1 {
		page = 1
	}

	offset := limit * (page - 1)

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count_query", rawQuery)
	result := query.db.Raw(countQuery, args...).Scan(&count)
	if result.Error != nil {
		return dtos, 0, result.Error
	}

	var items []E
	paginatedQuery := fmt.Sprintf("%s LIMIT %d OFFSET %d", rawQuery, limit, offset)
	result = query.db.Raw(paginatedQuery, args...).Scan(&items)
	if result.Error != nil {
		return dtos, count, result.Error
	}

	dtos = make([]M, 0)
	for _, item := range items {
		var dto M
		if err := dtoMapper.Map(&dto, item); err != nil {
			return dtos, count, err
		}
		dtos = append(dtos, dto)
	}

	return dtos, count, nil
}
