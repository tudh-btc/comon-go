package reposity

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	dtoMapper "github.com/dranikpg/dto-mapper"
	"github.com/go-playground/validator/v10"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// Todo: use gorm smart select, then no need for mapping

var Connected bool = false

var defaultDB *gorm.DB

type SQLQuery[M any, E any] struct {
	expressStr string
	args       []interface{}
	db         *gorm.DB
}

func Connect(sqlHost, sqlPort, sqlDbName, sqlSslmode, sqlUser, sqlPassword, currentSchema string) error {
	sqlDsn := fmt.Sprintf("host=%s port=%s dbname=%s sslmode=%s user=%s password=%s",
		sqlHost, sqlPort, sqlDbName, sqlSslmode, sqlUser, sqlPassword)
	//sqlDsn := "host=" + sqlUri + " dbname=" + sqlDatabase + " user=" + sqlUser + " password=" + sqlPassword

	database, err := gorm.Open(postgres.New(postgres.Config{
		DSN: sqlDsn,
	}), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   currentSchema + ".", // schema name
			SingularTable: true,                // use singular table name, table for `User` would be `user` with this option enabled
			//NoLowerCase:   true,                // skip the snake_casing of names
		}})

	if err != nil {
		panic("Failed to connect to database!")
	}

	// Add uuid-ossp extension for postgres database
	database.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
	/*
		// Migrate tables
		if len(autoMigrateModelList) > 0 {
			err = database.AutoMigrate(autoMigrateModelList...)
			if err != nil {
				panic("Failed to AutoMigrate table! err: " + err.Error())
			}
		}
	*/
	defaultDB = database
	Connected = true

	/*
		Todo: set connection Pool
			// Get generic database object sql.DB to use its functions
			sqlDB, err := defaultDB.DB()
			if err != nil {
				return err
			}
			// SetMaxIdleConns sets the maximum number of connections in the idle connection pool.
			sqlDB.SetMaxIdleConns(10)

			// SetMaxOpenConns sets the maximum number of open connections to the database.
			sqlDB.SetMaxOpenConns(100)

			// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
			sqlDB.SetConnMaxLifetime(time.Hour)
	*/
	// Todo: optimize performance https://gorm.io/docs/performance.html
	return nil
}

func Migrate(models ...interface{}) error {
	if !Connected {
		return errors.New("database not connected")
	}

	err := defaultDB.AutoMigrate(models...)
	return err
}

func Ping() error {
	if !Connected {
		return errors.New("not connected")
	}
	sqlDB, err := defaultDB.DB()
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

func Close() error {
	if !Connected {
		return errors.New("not connected")
	}
	sqlDB, err := defaultDB.DB()
	if err != nil {
		return err
	}
	err = sqlDB.Close()
	if err != nil {
		return err
	}
	Connected = false
	return nil
}

func Stats() (stats sql.DBStats, err error) {
	if !Connected {
		return stats, errors.New("not connected")
	}
	// Returns database statistics
	sqlDB, err := defaultDB.DB()
	if err != nil {
		return stats, err
	}
	return sqlDB.Stats(), nil
}

// NewQuery create new query instance
func NewQuery[M any, E any]( /*db *gorm.DB*/ dbInstances ...interface{}) *SQLQuery[M, E] {
	query := &SQLQuery[M, E]{}

	var isDBInitiallized bool = false
	for _, db := range dbInstances {
		if db != nil {
			switch t := db.(type) {
			case *gorm.DB:
				if t != nil {
					query.db = t
					isDBInitiallized = true
				}
			default:
			}
		}
	}

	// Assign default DB
	if !isDBInitiallized && defaultDB != nil {
		query.db = defaultDB
		isDBInitiallized = true
	}
	if !isDBInitiallized {
		panic(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> database is not initialized")
	}

	query.expressStr = ""
	query.args = make([]interface{}, 0)
	return query
}

// AddConditionOfTextField add one filter condition of normal text field into query
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

// AddTwoConditionOfTextField add two filter condition of two normal text field into query
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

// AddConditionOfJsonbField add one filter condition of jsonb field into query
func (query *SQLQuery[M, E]) AddConditionOfJsonbField(cascadingLogic string, fieldName string, key string, comparisonOperator string, value interface{}) {
	if fieldName == "" {
		return
	}

	if query.expressStr == "" {
		if comparisonOperator == "LIKE" {
			query.expressStr = fmt.Sprintf("lower(\"%s\") ->> '%s' %s ?", fieldName, key, comparisonOperator)
		} else {
			query.expressStr = fmt.Sprintf("\"%s\" ->> '%s' %s ?", fieldName, key, comparisonOperator)
		}
	} else {
		if comparisonOperator == "LIKE" {
			query.expressStr = fmt.Sprintf("%s %s lower(\"%s\") ->> '%s' %s ?", query.expressStr, cascadingLogic, fieldName, key, comparisonOperator)
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

// Exec run the the query to get all items with current filter, no paging
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

	// Query with filter
	var items []E
	result := defaultDB.Order(sort).Where(query.expressStr, query.args...).Find(&items)
	if result.Error != nil {
		return dtos, count, result.Error
	}
	//count = result.RowsAffected

	// Map entity item to DTO model
	dtos = make([]M, 0)
	for _, item := range items {
		// Mapping from entity model to DTO model
		var dto M
		if err := dtoMapper.Map(&dto, item); err != nil {
			return dtos, count, err
		}
		dtos = append(dtos, dto)
		count++
	}

	// Return
	return dtos, count, result.Error
}

// ExecPaging run the the query to get items with current filter, with paging
func (query *SQLQuery[M, E]) ExecWithPaging(sort string, limit int, page int) (dtos []M, count int64, err error) {
	if !Connected {
		return dtos, 0, errors.New("database not connected")
	}

	// Validate query param
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

	// Calculate offset
	offset := limit * (page - 1)
	var result *gorm.DB

	// Count total number
	var entityModel E
	result = query.db.Model(entityModel).Where(query.expressStr, query.args...).Count(&count)
	if result.Error != nil {
		return dtos, 0, result.Error
	}

	// Query with filter
	var items []E
	result = query.db.Limit(limit).Offset(offset).Order(sort).Where(query.expressStr, query.args...).Find(&items)

	// Map entity item to DTO model
	dtos = make([]M, 0)
	for _, item := range items {
		// Mapping from entity model to DTO model
		var dto M
		if err := dtoMapper.Map(&dto, item); err != nil {
			return dtos, count, err
		}
		dtos = append(dtos, dto)
	}

	return dtos, count, result.Error
}

// CreateItemFromDTO map dto (data transfer object) to new database's item struct
// and write that item into database , accepts generic types
//
// It return created item and error
func CreateItemFromDTO[M any, E any](dto M) (M, error) {
	if !Connected {
		return dto, errors.New("database not connected")
	}

	// Validate dto object  input
	validate := validator.New()
	err := validate.Struct(dto)
	if err != nil {
		return dto, err
	}

	// Mapping from DTO to entity model
	var item E
	if err := dtoMapper.Map(&item, dto); err != nil {
		return dto, err
	}

	// Create new entity using smart select
	var entity E
	if result := defaultDB.Model(entity).Create(&item); result.Error != nil {
		return dto, result.Error
	}

	// Mapping from entity model to DTO model
	if err := dtoMapper.Map(&dto, item); err != nil {
		return dto, err
	}
	return dto, nil
}

// ReadItemIntoDTO read an item by ID from database then map resutl into dto (data transfer object),
// accepts generic types
//
// It return read dto and error
func ReadItemByIDIntoDTO[M any, E any](id string) (dto M, err error) {
	if !Connected {
		return dto, errors.New("database not connected")
	}
	var item E
	if err := defaultDB.Where("id = ?", id).First(&item).Error; err != nil {
		return dto, err
	}

	// Mapping from entity model to DTO model
	if err := dtoMapper.Map(&dto, item); err != nil {
		return dto, err
	}
	return dto, nil
}

// ReadItemIntoDTO read an item by ID from database then map resutl into dto (data transfer object),
// accepts generic types
//
// It return read dtos and error
func ReadMultiItemsByIDIntoDTO[M any, E any](ids []string, sort string) (dtos []M, count int64, err error) {
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
	result := defaultDB.Order(sort).Where("id IN ?", ids).Find(&items)
	if result.Error != nil {
		return dtos, 0, result.Error
	}
	//count = result.RowsAffected

	dtos = make([]M, 0)
	for _, item := range items {
		// Mapping from entity model to DTO model
		var dto M
		if err := dtoMapper.Map(&dto, item); err != nil {
			return dtos, count, err
		}
		dtos = append(dtos, dto)
		count++
	}

	return dtos, count, nil
}

// ReadItemIntoDTO read an item by ID from database then map resutl into dto (data transfer object),
// accepts generic types
//
// It return read dtos and error
func ReadAllItemsIntoDTO[M any, E any](sort string) (dtos []M, count int64, err error) {
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

	result := defaultDB.Order(sort).Find(&items)
	if result.Error != nil {
		return dtos, 0, result.Error
	}
	//count = result.RowsAffected

	// Mapping from entity model to DTO model
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

// ReadItemWithFilterIntoDTO read an item with Filter from database then map resutl into dto (data transfer object),
// accepts generic types
//
// It return read dto and error
func ReadItemWithFilterIntoDTO[M any, E any](query string, args ...interface{}) (dto M, err error) {
	if !Connected {
		return dto, errors.New("database not connected")
	}

	var item E
	result := defaultDB.Where(query, args...).First(&item)
	if result.Error != nil {
		return dto, result.Error
	}

	// Mapping from entity model to DTO model
	if err := dtoMapper.Map(&dto, item); err != nil {
		return dto, err
	}

	return dto, nil
}

// UpdateItemByIDIntoDTO check if item ID exist in database, then map dto to item struct for updating it (actually patching),
// accepts generic types. Empty (null) field will not be updated
//
// It return updated item (dto) and error
func UpdateItemByIDFromDTO[M any, E any](id string, dto M) (M, error) {
	if !Connected {
		return dto, errors.New("database not connected")
	}

	// Check item exist by ID
	var item E
	if err := defaultDB.Where("id = ?", id).First(&item).Error; err != nil {
		return dto, err
	}

	// Mapping from DTO to entity model
	if err := dtoMapper.Map(&item, dto); err != nil {
		return dto, err
	}

	// Update item
	if err := defaultDB.Model(item).Where("id = ?", id).Updates(&item).Error; err != nil {
		return dto, err
	}

	// Mapping back from updated entity to DTO
	if err := dtoMapper.Map(&dto, item); err != nil {
		return dto, err
	}

	// Todo: uuid of dto is not updated here, please make dto's id updated here
	return dto, nil
}

// DeleteItemByID delete item by ID,
// accepts generic types.
//
// It return error if there is any
func DeleteItemByID[E any](id string) (err error) {
	if !Connected {
		return errors.New("database not connected")
	}

	var item E
	if err = defaultDB.Where("id = ?", id).Delete(&item).Error; err != nil {
		return err
	}

	return nil
}

// DeleteAllItem delete all item,
// accepts generic types.
//
// It return error if there is any
func DeleteAllItem[E any](softDelete bool) (err error) {
	if !Connected {
		return errors.New("database not connected")
	}

	var item E
	if softDelete {
		// Softdelete: the record WON'T be removed from the database,
		// but GORM will set the DeletedAt's value to the current time,
		// and the data is not findable with normal Query methods anymore.
		// You can find soft deleted records with Unscoped:
		// db.Unscoped().Where("age = 20").Find(&user)
		if err = defaultDB.Where("created_at > ?", "2000-01-01 00:00:00").Delete(&item).Error; err != nil {
			return err
		}
	} else {
		if err = defaultDB.Unscoped().Where("created_at > ?", "2000-01-01 00:00:00").Delete(&item).Error; err != nil {
			return err
		}
	}

	return nil
}

// CheckItemExistedByID check item is existed by ID,
// accepts generic types.
//
// It return true if item is existed
func CheckItemExistedByID[E any](id string) (exists bool, err error) {
	if !Connected {
		return exists, errors.New("database not connected")
	}

	var item E
	if err = defaultDB.Model(item).Select("count(*) > 0").Where("id = ?", id).Find(&exists).Error; err != nil {
		return exists, err
	}

	return exists, nil
}

// UpdateSingleColumn check if item ID exist in database, then updating it (actually patching),
// accepts generic types. Empty (null) field will not be updated
//
// It return error
func UpdateSingleColumn[E any](id string, columnName string, value interface{}) error {
	if !Connected {
		return errors.New("database not connected")
	}
	// Check item exist by ID
	var item E
	if err := defaultDB.Where("id = ?", id).First(&item).Error; err != nil {
		return err
	}

	// Update item
	if err := defaultDB.Model(item).Where("id = ?", id).Update(columnName, value).Error; err != nil {
		return err
	}

	return nil
}

//===============================

// AddJoin adds a JOIN clause to the query for joining multiple tables
func (query *SQLQuery[M, E]) AddJoin(joinType, table, condition string) *SQLQuery[M, E] {
	query.db = query.db.Joins(fmt.Sprintf("%s %s ON %s", joinType, table, condition))
	return query
}

// AddPreload adds a Preload clause to eagerly load related data
func (query *SQLQuery[M, E]) AddPreload(relation string) *SQLQuery[M, E] {
	query.db = query.db.Preload(relation)
	return query
}

// ExecCustomQuery executes a custom SQL query with support for multiple tables
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

// ExecCustomQueryWithPaging executes a custom SQL query with pagination support
func (query *SQLQuery[M, E]) ExecCustomQueryWithPaging(rawQuery string, limit, page int, args ...interface{}) (dtos []M, count int64, err error) {
	if !Connected {
		return dtos, 0, errors.New("database not connected")
	}

	// Validate query parameters
	if limit < 1 {
		limit = 100
	}
	if page < 1 {
		page = 1
	}

	// Calculate offset
	offset := limit * (page - 1)

	// Count total number
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count_query", rawQuery)
	result := query.db.Raw(countQuery, args...).Scan(&count)
	if result.Error != nil {
		return dtos, 0, result.Error
	}

	// Execute query with pagination
	var items []E
	paginatedQuery := fmt.Sprintf("%s LIMIT %d OFFSET %d", rawQuery, limit, offset)
	result = query.db.Raw(paginatedQuery, args...).Scan(&items)
	if result.Error != nil {
		return dtos, count, result.Error
	}

	// Map entity items to DTOs
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
