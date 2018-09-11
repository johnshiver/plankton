package config

import (
	"log"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/spf13/viper"
)

var c Config

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

}

type Config struct {
	DataBase *gorm.DB
}

func GetConfig() Config {
	return c
}

func ReadConfig() {
	err := viper.ReadInConfig()
	log.Printf("Using configuration file: %s\n", viper.ConfigFileUsed())

	if err != nil {
		log.Fatal(err.Error())
	}

}

func SetDataBase(db *gorm.DB) {
	if err := db.DB().Ping(); err != nil {
		log.Fatal(err)
		exit(1)
	}
	c.DataBase = db
}
