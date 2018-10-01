package config

import (
	"log"
	"os"

	"github.com/jinzhu/gorm"

	// _ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/spf13/viper"
)

// TODO: make this concurrency safe / dont let writes happen out of this module

var c Config

const (
	DEFAULT_CONCURRENCY_LIMIT   = 4
	DEFAULT_RESULT_CHANNEL_SIZE = 10000
)

var TEST_SQLITE_DATABASE = DatabaseConfig{
	Type: "sqlite3",
	Host: "/tmp/plankton_test.db",
}

var DEFAULT_SQLITE_DATABASE = DatabaseConfig{
	Type: "sqlite3",
	Host: "/tmp/plankton.db",
}

type Config struct {
	DataBase          *gorm.DB
	DBConfig          DatabaseConfig
	ConcurrencyLimit  int
	ResultChannelSize int
}

type DatabaseConfig struct {
	Type string
	Host string
}

func init() {
	viper.SetConfigType("yml")
	viper.SetConfigName("config")
	viper.AddConfigPath("/etc/plankton")
	viper.AddConfigPath(".")
	ReadAndSetConfig()
}

func GetConfig() Config {
	if c == (Config{}) {
		ReadAndSetConfig()
	}
	return c
}

func ReadAndSetConfig() {
	err := viper.ReadInConfig()
	log.Printf("Using configuration file: %s\n", viper.ConfigFileUsed())

	// config file didnt work, use defaults
	if err != nil {
		log.Println(err.Error())
		log.Println("Using default sqlite db")
		SetDatabaseConfig(DEFAULT_SQLITE_DATABASE)
		c.ConcurrencyLimit = DEFAULT_CONCURRENCY_LIMIT
		return
	}
	concurrencyLimit := viper.GetInt("ConcurrencyLimit")
	if concurrencyLimit < 1 {
		concurrencyLimit = DEFAULT_CONCURRENCY_LIMIT
	}
	c.ConcurrencyLimit = concurrencyLimit
	c.ResultChannelSize = DEFAULT_RESULT_CHANNEL_SIZE
	databaseType := viper.GetString("DatabaseType")
	databaseHost := viper.GetString("DatabaseHost")
	SetDatabaseConfig(DatabaseConfig{
		Type: databaseType,
		Host: databaseHost,
	})

}

func SetDatabaseConfig(db_config DatabaseConfig) {
	c.DBConfig = db_config
	db, err := gorm.Open(c.DBConfig.Type, c.DBConfig.Host)
	if err != nil {
		panic(err)
	}

	if err := db.DB().Ping(); err != nil {
		log.Fatal("Error connecting to database")
		log.Fatal(err)
		os.Exit(1)
	}
	c.DataBase = db
}
