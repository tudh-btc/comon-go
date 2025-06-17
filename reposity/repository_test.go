package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func TestRepository(t *testing.T) {
	// Khởi tạo container PostgreSQL
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
	require.NoError(t, err, "không thể khởi tạo container PostgreSQL")
	defer postgresContainer.Terminate(ctx)

	// Lấy host và port từ container
	host, err := postgresContainer.Host(ctx)
	require.NoError(t, err, "không thể lấy host từ container")
	port, err := postgresContainer.MappedPort(ctx, "5432")
	require.NoError(t, err, "không thể lấy port từ container")

	// Thiết lập tham số kết nối
	sqlHost := host
	sqlPort := port.Port()
	sqlDbName := "testdb"
	sqlSslmode := "disable"
	sqlUser := "testuser"
	sqlPassword := "testpass"
	schemas := []string{"schema1", "schema2"}

	// Tạo schema trong cơ sở dữ liệu
	dsn := fmt.Sprintf("host=%s port=%s dbname=%s sslmode=%s user=%s password=%s",
		sqlHost, sqlPort, sqlDbName, sqlSslmode, sqlUser, sqlPassword)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err, "không thể kết nối tới cơ sở dữ liệu để tạo schema")
	for _, schema := range schemas {
		err = db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema)).Error
		require.NoError(t, err, "không thể tạo schema %s", schema)
	}

	// Hàm Connect sửa đổi cho test với ConnMaxIdleTime ngắn hơn
	connectForTest := func(sqlHost, sqlPort, sqlDbName, sqlSslmode, sqlUser, sqlPassword string, schemas []string) error {
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
			sqlDB.SetConnMaxIdleTime(1 * time.Second) // Giảm ConnMaxIdleTime cho test

			stats := sqlDB.Stats()
			if stats.MaxOpenConnections != 100 {
				fmt.Printf("Cảnh báo: MaxOpenConnections cho schema %s không được thiết lập đúng, kỳ vọng 100, nhận được %d\n", currentSchema, stats.MaxOpenConnections)
			}

			dbMap[currentSchema] = database
		}

		defaultSchema = schemas[0]
		Connected = true
		return nil
	}

	t.Run("Connect_Success", func(t *testing.T) {
		err := connectForTest(sqlHost, sqlPort, sqlDbName, sqlSslmode, sqlUser, sqlPassword, schemas)
		require.NoError(t, err, "Connect thất bại")
		require.True(t, Connected, "Connected phải là true sau khi kết nối thành công")
		require.Equal(t, schemas[0], defaultSchema, "defaultSchema phải là schema đầu tiên")
		require.Len(t, dbMap, len(schemas), "dbMap phải chứa đúng số lượng schema")

		for _, schema := range schemas {
			sqlDB, err := dbMap[schema].DB()
			require.NoError(t, err, "không thể lấy sql.DB cho schema %s", schema)
			stats := sqlDB.Stats()
			require.Equal(t, 100, stats.MaxOpenConnections, "MaxOpenConnections phải là 100")

			for i := 0; i < 15; i++ {
				var result int
				sqlDB.QueryRow("SELECT 1").Scan(&result)
			}
			time.Sleep(100 * time.Millisecond)
			stats = sqlDB.Stats()
			require.LessOrEqual(t, stats.Idle, 10, "Số kết nối nhàn rỗi không được vượt quá MaxIdleConns (10)")
		}
	})

	t.Run("Connect_EmptySchemas", func(t *testing.T) {
		Close()
		err := connectForTest(sqlHost, sqlPort, sqlDbName, sqlSslmode, sqlUser, sqlPassword, []string{})
		require.Error(t, err, "Connect phải trả về lỗi khi danh sách schema rỗng")
		require.Equal(t, "phải cung cấp ít nhất một schema", err.Error(), "Thông báo lỗi không đúng")
		require.False(t, Connected, "Connected phải là false khi Connect thất bại")
	})

	t.Run("Ping_Success", func(t *testing.T) {
		err := connectForTest(sqlHost, sqlPort, sqlDbName, sqlSslmode, sqlUser, sqlPassword, schemas)
		require.NoError(t, err, "Connect thất bại trước khi ping")

		for _, schema := range schemas {
			err = Ping(schema)
			require.NoError(t, err, "Ping thất bại cho schema %s", schema)
		}
	})

	t.Run("Ping_InvalidSchema", func(t *testing.T) {
		err := Ping("invalid_schema")
		require.Error(t, err, "Ping phải trả về lỗi khi schema không tồn tại")
		require.Contains(t, err.Error(), "schema invalid_schema not connected", "Thông báo lỗi không đúng")
	})

	t.Run("Close_Success", func(t *testing.T) {
		err := connectForTest(sqlHost, sqlPort, sqlDbName, sqlSslmode, sqlUser, sqlPassword, schemas)
		require.NoError(t, err, "Connect thất bại trước khi đóng")

		err = Close()
		require.NoError(t, err, "Close thất bại")
		require.False(t, Connected, "Connected phải là false sau khi đóng")
		require.Empty(t, dbMap, "dbMap phải rỗng sau khi đóng")
		require.Empty(t, defaultSchema, "defaultSchema phải rỗng sau khi đóng")
	})

	t.Run("Close_NotConnected", func(t *testing.T) {
		Close()
		err := Close()
		require.Error(t, err, "Close phải trả về lỗi khi không có kết nối")
		require.Equal(t, "not connected", err.Error(), "Thông báo lỗi không đúng")
	})

	t.Run("Stats_Success", func(t *testing.T) {
		err := connectForTest(sqlHost, sqlPort, sqlDbName, sqlSslmode, sqlUser, sqlPassword, schemas)
		require.NoError(t, err, "Connect thất bại trước khi lấy stats")

		for _, schema := range schemas {
			stats, err := Stats(schema)
			require.NoError(t, err, "Stats thất bại cho schema %s", schema)
			require.Equal(t, 100, stats.MaxOpenConnections, "MaxOpenConnections phải là 100")

			sqlDB, err := dbMap[schema].DB()
			require.NoError(t, err, "không thể lấy sql.DB cho schema %s", schema)
			for i := 0; i < 15; i++ {
				var result int
				sqlDB.QueryRow("SELECT 1").Scan(&result)
			}
			time.Sleep(100 * time.Millisecond)
			stats = sqlDB.Stats()
			require.LessOrEqual(t, stats.Idle, 10, "Số kết nối nhàn rỗi không được vượt quá MaxIdleConns (10)")
		}
	})

	t.Run("Stats_InvalidSchema", func(t *testing.T) {
		_, err := Stats("invalid_schema")
		require.Error(t, err, "Stats phải trả về lỗi khi schema không tồn tại")
		require.Contains(t, err.Error(), "schema invalid_schema not connected", "Thông báo lỗi không đúng")
	})

	t.Run("Stats_NotConnected", func(t *testing.T) {
		Close()
		_, err := Stats(schemas[0])
		require.Error(t, err, "Stats phải trả về lỗi khi không có kết nối")
		require.Equal(t, "not connected", err.Error(), "Thông báo lỗi không đúng")
	})

	t.Run("ConnectionPool_Behavior", func(t *testing.T) {
		err := connectForTest(sqlHost, sqlPort, sqlDbName, sqlSslmode, sqlUser, sqlPassword, schemas)
		require.NoError(t, err, "Connect thất bại")

		for _, schema := range schemas {
			sqlDB, err := dbMap[schema].DB()
			require.NoError(t, err, "không thể lấy sql.DB cho schema %s", schema)

			// Mô phỏng sử dụng nhiều kết nối
			for i := 0; i < 2000; i++ {
				go func() {
					var result int
					sqlDB.QueryRow("SELECT 1").Scan(&result)
				}()
			}
			time.Sleep(500 * time.Millisecond)

			// Kiểm tra MaxIdleConns
			stats := sqlDB.Stats()
			require.LessOrEqual(t, stats.Idle, 10, "Số kết nối nhàn rỗi không được vượt quá 10")

			// Kiểm tra ConnMaxIdleTime
			time.Sleep(5 * time.Second) // Đợi lâu hơn ConnMaxIdleTime (1 giây)
			stats = sqlDB.Stats()
			require.Greater(t, stats.MaxIdleTimeClosed, int64(0), "Phải có kết nối bị đóng do ConnMaxIdleTime")

			// Kiểm tra ConnMaxLifetime (giả lập)
			for i := 0; i < 5; i++ {
				var result int
				sqlDB.QueryRow("SELECT 1").Scan(&result)
				time.Sleep(1 * time.Second)
			}
			stats = sqlDB.Stats()
			if stats.MaxLifetimeClosed > 0 {
				t.Logf("Có %d kết nối bị đóng do ConnMaxLifetime", stats.MaxLifetimeClosed)
			}
		}
	})
}
