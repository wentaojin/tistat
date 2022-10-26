/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package server

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/wentaojin/tistat/config"
	"github.com/wentaojin/tistat/logger"
	"github.com/wentaojin/tistat/utils"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	_ "github.com/go-sql-driver/mysql"
	"gorm.io/gorm/schema"
)

type Engine struct {
	TiDB   *sql.DB
	GormDB *gorm.DB
}

func NewMySQLEngineDB(mysqlCfg *config.Config) (*Engine, error) {
	// meta-schema 创建
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?%s",
		mysqlCfg.TiDBConfig.Username,
		mysqlCfg.TiDBConfig.Password,
		mysqlCfg.TiDBConfig.Host,
		mysqlCfg.TiDBConfig.Port,
		mysqlCfg.TiDBConfig.ConnectParams)
	createDB, err := gorm.Open(mysql.New(mysql.Config{
		DriverName: "mysql",
		DSN:        dbDSN,
	}), &gorm.Config{})
	if err != nil {
		return &Engine{}, fmt.Errorf("error on create mysql database connection [%v]: %v", mysqlCfg.TiDBConfig.MetaSchema, err)
	}

	crtDB, err := createDB.DB()
	if err != nil {
		return &Engine{}, fmt.Errorf("error on gormDB.DB() convert crtDB failed [%v]: %v",
			mysqlCfg.TiDBConfig.MetaSchema, err)
	}

	if _, err = crtDB.Exec(utils.StringsBuilder(`CREATE DATABASE IF NOT EXISTS `, mysqlCfg.TiDBConfig.MetaSchema)); err != nil {
		return &Engine{}, fmt.Errorf("error on gormDB.DB() create meta-schema failed [%v]: %v",
			mysqlCfg.TiDBConfig.MetaSchema, err)
	}

	if err = crtDB.Close(); err != nil {
		return &Engine{}, fmt.Errorf("error on gormDB.DB() close crtDB failed [%v]: %v",
			mysqlCfg.TiDBConfig.MetaSchema, err)
	}

	// 数据表以及数据库连接创建
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		mysqlCfg.TiDBConfig.Username,
		mysqlCfg.TiDBConfig.Password,
		mysqlCfg.TiDBConfig.Host,
		mysqlCfg.TiDBConfig.Port,
		mysqlCfg.TiDBConfig.MetaSchema,
		mysqlCfg.TiDBConfig.ConnectParams)

	var (
		gormDB *gorm.DB
	)
	// 初始化 gormDB
	// 初始化 gorm 日志记录器
	log := logger.NewGormLogger(zap.L(), mysqlCfg.AppConfig.SlowLogThreshold)
	log.SetAsDefault()
	gormDB, err = gorm.Open(mysql.New(mysql.Config{
		DriverName: "mysql",
		DSN:        dsn,
	}), &gorm.Config{
		// 禁用外键（指定外键时不会在 mysql 创建真实的外键约束）
		DisableForeignKeyConstraintWhenMigrating: true,
		PrepareStmt:                              true,
		Logger:                                   log,
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
	})
	if err != nil {
		return &Engine{}, fmt.Errorf("error on initializing mysql database connection [%v]: %v", mysqlCfg.TiDBConfig.MetaSchema, err)
	}

	sqlDB, err := gormDB.DB()
	if err != nil {
		return &Engine{}, fmt.Errorf("error on gormDB.DB() convert sqlDB failed [%v]: %v",
			mysqlCfg.TiDBConfig.MetaSchema, err)
	}

	sqlDB.SetMaxIdleConns(utils.MySQLMaxIdleConns)
	sqlDB.SetMaxOpenConns(utils.MySQLMaxOpenConns)
	sqlDB.SetConnMaxLifetime(utils.MySQLConnMaxLifetime)

	if err = sqlDB.Ping(); err != nil {
		return &Engine{}, fmt.Errorf("error on ping mysql database connection failed [%v]: %v",
			mysqlCfg.TiDBConfig.MetaSchema, err)
	}

	zap.L().Info("new mysql engine db",
		zap.String("meta-schema", mysqlCfg.TiDBConfig.MetaSchema),
		zap.Int("mysql idle conn", mysqlCfg.AppConfig.TableThreads),
		zap.Int("mysql max open conn", mysqlCfg.AppConfig.TableThreads),
		zap.Duration("mysql conn max life time", time.Duration(mysqlCfg.AppConfig.AnalyzeTimeout)*time.Second))
	return &Engine{
		TiDB:   sqlDB,
		GormDB: gormDB,
	}, nil
}

func (e *Engine) InitMySQLEngineDB() error {
	if !e.GormDB.Migrator().HasTable(&WaitStatsTable{}) {
		if err := e.GormDB.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci").AutoMigrate(
			&WaitStatsTable{},
		); err != nil {
			return fmt.Errorf("error on init mysql engine table [wait_stats_table] failed: %v", err)
		}
	}
	if !e.GormDB.Migrator().HasTable(&StatsHealthyBefore{}) {
		if err := e.GormDB.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci").AutoMigrate(
			&StatsHealthyBefore{},
		); err != nil {
			return fmt.Errorf("error on init mysql engine table [stats_healthy_before] failed: %v", err)
		}
	}
	if !e.GormDB.Migrator().HasTable(&StatsHealthyAfter{}) {
		if err := e.GormDB.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci").AutoMigrate(
			&StatsHealthyAfter{},
		); err != nil {
			return fmt.Errorf("error on init mysql engine table [stats_healthy_after] failed: %v", err)
		}
	}
	if !e.GormDB.Migrator().HasTable(&StatsHistogramsBefore{}) {
		if err := e.GormDB.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci").AutoMigrate(
			&StatsHistogramsBefore{},
		); err != nil {
			return fmt.Errorf("error on init mysql engine table [stats_histograms_before] failed: %v", err)
		}
	}
	if !e.GormDB.Migrator().HasTable(&StatsHistogramsAfter{}) {
		if err := e.GormDB.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci").AutoMigrate(
			&StatsHistogramsAfter{},
		); err != nil {
			return fmt.Errorf("error on init mysql engine table [stats_histograms_after] failed: %v", err)
		}
	}
	if !e.GormDB.Migrator().HasTable(&StatsMetaBefore{}) {
		if err := e.GormDB.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci").AutoMigrate(
			&StatsMetaBefore{},
		); err != nil {
			return fmt.Errorf("error on init mysql engine table [stats_meta_before] failed: %v", err)
		}
	}
	if !e.GormDB.Migrator().HasTable(&StatsMetaAfter{}) {
		if err := e.GormDB.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci").AutoMigrate(
			&StatsMetaAfter{},
		); err != nil {
			return fmt.Errorf("error on init mysql engine table [stats_meta_after] failed: %v", err)
		}
	}
	if !e.GormDB.Migrator().HasTable(&StatsTriggerRule{}) {
		if err := e.GormDB.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci").AutoMigrate(
			&StatsTriggerRule{},
		); err != nil {
			return fmt.Errorf("error on init mysql engine table [stats_trigger_rule] failed: %v", err)
		}
	}
	if !e.GormDB.Migrator().HasTable(&StatsAnalyzeRule{}) {
		if err := e.GormDB.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci").AutoMigrate(
			&StatsAnalyzeRule{},
		); err != nil {
			return fmt.Errorf("error on init mysql engine table [stats_analyze_rule] failed: %v", err)
		}
	}

	zap.L().Info("init mysql engine db",
		zap.Strings("tables", []string{
			"wait_stats_table",
			"stats_meta_before",
			"stats_meta_after",
			"stats_histograms_before",
			"stats_histograms_after",
			"stats_healthy_before",
			"stats_healthy_after",
			"stats_trigger_rule",
			"stats_analyze_rule"}))
	return nil
}

func (e *Engine) Query(querySQL string) ([]string, []map[string]string, error) {
	var (
		columns []string
		results []map[string]string
	)
	rows, err := e.TiDB.Query(querySQL)
	if err != nil {
		return columns, results, fmt.Errorf("error on query general sql [%v] failed: [%v]", querySQL, err.Error())
	}
	defer rows.Close()

	//不确定字段通用查询，自动获取字段名称
	columns, err = rows.Columns()
	if err != nil {
		return columns, results, fmt.Errorf("error on query general sql [%v] rows.Columns failed: [%v]", querySQL, err.Error())
	}

	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return columns, results, fmt.Errorf("error on query general sql [%v] rows.Scan failed: [%v]", querySQL, err.Error())
		}

		row := make(map[string]string)
		for k, v := range values {
			key := columns[k]
			// 数据库类型 MySQL NULL 是 NULL，空字符串是空字符串
			// 数据库类型 Oracle NULL、空字符串归于一类 NULL
			// Oracle/Mysql 对于 'NULL' 统一字符 NULL 处理，查询出来转成 NULL,所以需要判断处理
			if v == nil { // 处理 NULL 情况，当数据库类型 MySQL 等于 nil
				row[key] = "NULL"
			} else {
				// 处理空字符串以及其他值情况
				// 数据统一 string 格式显示
				row[key] = string(v)
			}
		}
		results = append(results, row)
	}

	if err = rows.Err(); err != nil {
		return columns, results, fmt.Errorf("error on query general sql [%v] rows.Next failed: [%v]", querySQL, err.Error())

	}
	return columns, results, nil
}
