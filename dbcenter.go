package dbcenter

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"database/sql"

	"github.com/globalsign/mgo"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gomodule/redigo/redis"
)

// DB center types
const (
	MYSQL string = "MYSQL"
	MONGO string = "MONGO"
	REDIS string = "REDIS"

	WRONG_DBTYPE     string = "Wrong DB Type."
	SAME_DB_IDENTIFY string = "Same DB Identify."
	No_DB_IDENTIFY   string = "No DB Identify."
)

type DBCenter struct {
	MySQL map[string]*sql.DB
	Mongo map[string]*mgo.Session
	Redis map[string]*redis.Pool
}

var dbCenter *DBCenter

type DBConfig struct {
	Identify      string // 辨識
	Host          string
	UserName      string        //資料庫帳號
	Password      string        //資料庫密碼
	DBName        string        //資料庫名稱
	MaxConn       int           //最大連線數
	MaxIdleConn   int           //最大連閒致線數
	MaxLifetime   int           //Sec
	MaxIdleTimeMS int           // maximum number of milliseconds that a connection can remain idle in the pool
	PoolLimit     int           // PoolLimit defines the per-server socket pool limit.
	MinPoolSize   int           // MinPoolSize defines The minimum number of connections in the connection pool
	ReadTimeout   time.Duration // I/O read timeout
	WriteTimeout  time.Duration // I/O write timeout
}

func init() {
	dbCenter = &DBCenter{}
	dbCenter.MySQL = make(map[string]*sql.DB)
	dbCenter.Mongo = make(map[string]*mgo.Session)
	dbCenter.Redis = make(map[string]*redis.Pool)
}

func GetPool() *DBCenter {
	return dbCenter
}

func (dc *DBCenter) NewDB(dbType string, conf DBConfig) error {
	switch dbType {
	case MYSQL:
		if _, exist := db.MySQL[conf.Identify]; exist {
			return errors.New(SAME_DB_IDENTIFY)
		}
		return db.newMariaDB(conf)

	// case MONGO:
	// 	if _, exist := db.Mongo[conf.Identify]; exist {
	// 		return errors.New(SAME_DB_IDENTIFY)
	// 	}
	// 	return db.newMongoDB(conf)

	// case REDIS:
	// 	if _, exist := db.Redis[conf.Identify]; exist {
	// 		return errors.New(SAME_DB_IDENTIFY)
	// 	}
	// 	return db.newRedis(conf)

	default:
		return errors.New(WRONG_DBTYPE)

	}
	return nil
}

func (db *DBCenter) newMySQL(config DBConfig) error {
	connStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8&parseTime=true&loc=UTC&timeout=30s", config.UserName, config.Password, config.Host, config.DBName)

	readTimeoutSeconds := config.ReadTimeout.Seconds()
	if readTimeoutSeconds > 0 {
		connStr += "&readTimeout=" + strconv.Itoa(int(readTimeoutSeconds)) + "s"
	}

	writeTimeoutSeconds := config.WriteTimeout.Seconds()
	if writeTimeoutSeconds > 0 {
		connStr += "&writeTimeout=" + strconv.Itoa(int(writeTimeoutSeconds)) + "s"
	}

	tempdb, err := sql.Open("mysql", connStr)
	if err != nil {
		return err
	}

	if config.MaxConn <= 0 {
		config.MaxConn = 50
	}

	if config.MaxIdleConn <= 0 {
		config.MaxIdleConn = 25
	}

	if config.MaxLifetime <= 0 {
		config.MaxLifetime = 600
	}
	//10 * time.Minute
	tempdb.DB().SetMaxOpenConns(config.MaxConn)
	tempdb.DB().SetMaxIdleConns(config.MaxIdleConn)
	tempdb.DB().SetConnMaxLifetime(time.Duration(config.MaxLifetime) * time.Second)
	db.MySQL[config.Identify] = tempdb

	return nil
}

func (db *DBCenter) GetMySQL(identify string) (db *sql.DB, err error) {
	if _, exist := db.MySQL[identify]; exist {
		db = db.MySQL[identify]
	} else {
		err = errors.New(No_DB_IDENTIFY)
	}

	return
}
