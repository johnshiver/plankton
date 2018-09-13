package config

import (
	"fmt"
	"log"
	"os"

	"github.com/jinzhu/gorm"

	// _ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/spf13/viper"
)

var c Config

var DEFAULT_DATABASE = DatabaseConfig{
	Type: "sqlite3",
	Host: "/tmp/plankton_test.db",
}

type Config struct {
	DataBase *gorm.DB
	DBConfig DatabaseConfig
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

	/*
			initalize config
		        from yaml, get the database config (for now sqlite or postgres)
			then determine if the connection string is valid
	*/
	readConfig()

}

func GetConfig() Config {
	if c == (Config{}) {
		readConfig()
	}
	return c
}

func readConfig() {
	err := viper.ReadInConfig()
	log.Printf("Using configuration file: %s\n", viper.ConfigFileUsed())

	if err != nil {
		log.Fatal(err.Error())
	}

	database_type := viper.GetString("database_type")
	database_host := viper.GetString("database_host")
	fmt.Println(database_type, database_host)

	SetDatabaseConfig(DatabaseConfig{
		Type: database_type,
		Host: database_host,
	})

	// db, err := gorm.Open("postgres", "host=127.0.0.1 port=5432 user=postgres dbname=mytestdb password=mysecretpassword sslmode=disable")
	db, err := gorm.Open(c.DBConfig.Type, c.DBConfig.Host)
	if err != nil {
		panic(err)
	}

	// TODO: might revisit this decision
	//db.AutoMigrate(&scheduler.PlanktonRecord{})
	SetDataBase(db)

}

func SetDataBase(db *gorm.DB) {
	if err := db.DB().Ping(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	c.DataBase = db
}

func SetDatabaseConfig(db_config DatabaseConfig) {
	c.DBConfig = db_config
}
