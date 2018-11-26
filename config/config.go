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

// --------------------------------- Constants -----------------------------------------
const (
	DEFAULT_CONCURRENCY_LIMIT   = 4
	DEFAULT_RESULT_CHANNEL_SIZE = 10000
	DEFAULT_VERSION             = "UNVERSIONED"
)

var DEFAULT_LOGGING_DIRECTORY = os.Getenv("HOME") + "/.plankton_logs/"

var TEST_SQLITE_DATABASE = DatabaseConfig{
	Type: "sqlite3",
	Host: "/tmp/plankton_test.db",
}

var DEFAULT_SQLITE_DATABASE = DatabaseConfig{
	Type: "sqlite3",
	Host: "/tmp/plankton.db",
}

// -------------------------------------------------------------------------------------

type Config struct {
	DataBase          *gorm.DB
	DBConfig          DatabaseConfig
	ConcurrencyLimit  int
	ResultChannelSize int
	LoggingDirectory  string
	Version           string
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
	ReadOrCreateLoggingDirectory()
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

	// config file didnt work, use the defaults
	if err != nil {
		log.Println(err.Error())
		log.Println("Using all default values")
		SetDatabaseConfig(DEFAULT_SQLITE_DATABASE)
		c.ConcurrencyLimit = DEFAULT_CONCURRENCY_LIMIT
		c.LoggingDirectory = DEFAULT_LOGGING_DIRECTORY
		c.ResultChannelSize = DEFAULT_RESULT_CHANNEL_SIZE
		c.Version = DEFAULT_VERSION
		return
	}

	version := viper.GetString("Version")
	if version == "" {
		version = DEFAULT_VERSION
	}
	c.Version = version

	concurrencyLimit := viper.GetInt("ConcurrencyLimit")
	if concurrencyLimit < 1 {
		concurrencyLimit = DEFAULT_CONCURRENCY_LIMIT
	}
	c.ConcurrencyLimit = concurrencyLimit

	resultChanSize := viper.GetInt("ResultChannelSize")
	if resultChanSize < 1 {
		resultChanSize = DEFAULT_RESULT_CHANNEL_SIZE
	}
	c.ResultChannelSize = resultChanSize

	loggingDir := viper.GetString("LoggingDirectory")
	if loggingDir == "" {
		loggingDir = DEFAULT_LOGGING_DIRECTORY
	}
	c.LoggingDirectory = loggingDir

	databaseType := viper.GetString("DatabaseType")
	databaseHost := viper.GetString("DatabaseHost")
	if databaseType == "" || databaseHost == "" {
		SetDatabaseConfig(DEFAULT_SQLITE_DATABASE)

	} else {
		SetDatabaseConfig(DatabaseConfig{
			Type: databaseType,
			Host: databaseHost,
		})

	}
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

func ReadOrCreateLoggingDirectory() {
	err := os.MkdirAll(c.LoggingDirectory, os.ModePerm)
	if err != nil {
		panic(err)
	}

}
