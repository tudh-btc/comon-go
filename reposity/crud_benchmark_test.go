package repository

import (
	"context"
	"fmt"
	"time"

	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// User và UserDTO dùng cho test
type User struct {
	ID        string `gorm:"primaryKey"`
	Name      string
	Email     string
	CreatedAt time.Time
}

type UserDTO struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// connectForTest thiết lập kết nối với ConnMaxIdleTime ngắn
func connectForTest(sqlHost, sqlPort, sqlDbName, sqlSslmode, sqlUser, sqlPassword string, schemas []string) error {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	if len(schemas) == 0 {
		return fmt.Errorf("phải cung cấp ít nhất một schema")
	}

	for _, currentSchema := range schemas {
		sqlDsn := fmt.Sprintf("host=%s port=%s dbname=%s sslmode=%s user=%s password=%s",
			sqlHost, sqlPort, sqlDbName, sqlSslmode, sqlUser, sqlPassword)

		database, err := gorm.Open(postgres.New(postgres.Config{
			DSN: sqlDsn,
		}), &gorm.Config{
			NamingStrategy: schema.NamingStrategy{
				TablePrefix:   currentSchema + ".",
				SingularTable: true,
			},
		})
		if err != nil {
			return fmt.Errorf("không thể kết nối tới cơ sở dữ liệu cho schema %s: %w", currentSchema, err)
		}

		database.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")

		sqlDB, err := database.DB()
		if err != nil {
			return fmt.Errorf("không thể lấy sql.DB cho schema %s: %w", currentSchema, err)
		}

		sqlDB.SetMaxIdleConns(10)
		sqlDB.SetMaxOpenConns(100)
		sqlDB.SetConnMaxLifetime(30 * time.Minute)
		sqlDB.SetConnMaxIdleTime(30 * time.Second) // Giảm ConnMaxIdleTime cho test

		dbMap[currentSchema] = database
	}

	defaultSchema = schemas[0]
	Connected = true
	return nil
}

// setupTestContainer thiết lập container PostgreSQL và tạo schema
func setupTestContainer(b *testing.B) (testcontainers.Container, string, string, []string) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:latest",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp"),
	}
	postgresContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(b, err, "không thể khởi tạo container PostgreSQL")

	host, err := postgresContainer.Host(ctx)
	require.NoError(b, err)
	port, err := postgresContainer.MappedPort(ctx, "5432")
	require.NoError(b, err)

	sqlHost := host
	sqlPort := port.Port()
	schemas := []string{"schema1"}

	// Tạo schema
	dsn := fmt.Sprintf("host=%s port=%s dbname=%s sslmode=%s user=%s password=%s",
		sqlHost, sqlPort, "testdb", "disable", "testuser", "testpass")
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(b, err)
	err = db.Exec("CREATE SCHEMA IF NOT EXISTS schema1").Error
	require.NoError(b, err)

	return postgresContainer, sqlHost, sqlPort, schemas
}

func BenchmarkCreateItemFromDTO(b *testing.B) {
	postgresContainer, sqlHost, sqlPort, schemas := setupTestContainer(b)
	defer postgresContainer.Terminate(context.Background())

	err := connectForTest(sqlHost, sqlPort, "testdb", "disable", "testuser", "testpass", schemas)
	require.NoError(b, err)

	err = Migrate("schema1", &User{})
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dto := UserDTO{
			ID:    uuid.New().String(),
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("user%d@example.com", i),
		}
		_, err := CreateItemFromDTO[UserDTO, User]("schema1", dto)
		require.NoError(b, err)
	}
}

func BenchmarkReadItemByIDIntoDTO(b *testing.B) {
	postgresContainer, sqlHost, sqlPort, schemas := setupTestContainer(b)
	defer postgresContainer.Terminate(context.Background())

	err := connectForTest(sqlHost, sqlPort, "testdb", "disable", "testuser", "testpass", schemas)
	require.NoError(b, err)

	err = Migrate("schema1", &User{})
	require.NoError(b, err)

	// Chèn dữ liệu mẫu
	dto := UserDTO{
		ID:    uuid.New().String(),
		Name:  "TestUser",
		Email: "test@example.com",
	}
	_, err = CreateItemFromDTO[UserDTO, User]("schema1", dto)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ReadItemByIDIntoDTO[UserDTO, User]("schema1", dto.ID)
		require.NoError(b, err)
	}
}

func BenchmarkUpdateItemByIDFromDTO(b *testing.B) {
	postgresContainer, sqlHost, sqlPort, schemas := setupTestContainer(b)
	defer postgresContainer.Terminate(context.Background())

	err := connectForTest(sqlHost, sqlPort, "testdb", "disable", "testuser", "testpass", schemas)
	require.NoError(b, err)

	err = Migrate("schema1", &User{})
	require.NoError(b, err)

	// Chèn dữ liệu mẫu
	dto := UserDTO{
		ID:    uuid.New().String(),
		Name:  "TestUser",
		Email: "test@example.com",
	}
	_, err = CreateItemFromDTO[UserDTO, User]("schema1", dto)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dto.Name = fmt.Sprintf("UpdatedUser%d", i)
		_, err := UpdateItemByIDFromDTO[UserDTO, User]("schema1", dto.ID, dto)
		require.NoError(b, err)
	}
}

func BenchmarkDeleteItemByID(b *testing.B) {
	postgresContainer, sqlHost, sqlPort, schemas := setupTestContainer(b)
	defer postgresContainer.Terminate(context.Background())

	err := connectForTest(sqlHost, sqlPort, "testdb", "disable", "testuser", "testpass", schemas)
	require.NoError(b, err)

	err = Migrate("schema1", &User{})
	require.NoError(b, err)

	// Chèn dữ liệu mẫu
	ids := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		dto := UserDTO{
			ID:    uuid.New().String(),
			Name:  "TestUser",
			Email: "test@example.com",
		}
		_, err = CreateItemFromDTO[UserDTO, User]("schema1", dto)
		require.NoError(b, err)
		ids[i] = dto.ID
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := DeleteItemByID[User]("schema1", ids[i])
		require.NoError(b, err)
	}
}
