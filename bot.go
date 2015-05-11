package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ChimeraCoder/anaconda"
	"github.com/garyburd/redigo/redis"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	ConfigFilename = "/Users/shinderuman2/src/golang/csv2tweet/config/bot1-2.json"
	CsvPath        = "/Users/shinderuman2/src/golang/csv2tweet/csv/"
	/*
		ConfigFilename = "/home/shinderuman/src/bot_config/config.json"
		CsvPath        = "/home/shinderuman/src/bot_csv/"
	*/
	TweetMaxLength = 140
)

type Config struct {
	Name                     string `json:"name"`
	Type                     string `json:"type"`
	Period                   int    `json:"period"`
	TwitterConsumerKey       string `json:"twitter_consumer_key"`
	TwitterConsumerSecret    string `json:"twitter_consumer_secret"`
	TwitterAccessToken       string `json:"twitter_access_token"`
	TwitterAccessTokenSecret string `json:"twitter_access_token_secret"`
	CsvFilename              string `json:"csv_filename"`
	StatusFormat             string `json:"status_format"`
	StatusColumns            string `json:"status_columns"`
	Enabled                  bool   `json:"enabled"`
}

func main() {
	file, err := ioutil.ReadFile(ConfigFilename)
	if err != nil {
		panic(err)
	}
	var configs []Config
	json.Unmarshal(file, &configs)
	for _, config := range configs {
		if config.Enabled != true {
			fmt.Printf("skipped(disabled) %s\n", config.Name)
			continue
		}
		var minute = time.Now().Hour()*60 + time.Now().Minute()
		if minute%config.Period != 0 {
			fmt.Printf("skipped(outside the period) %s\n", config.Name)
			continue
		}

		anaconda.SetConsumerKey(config.TwitterConsumerKey)
		anaconda.SetConsumerSecret(config.TwitterConsumerSecret)
		api := anaconda.NewTwitterApi(config.TwitterAccessToken, config.TwitterAccessTokenSecret)

		file, err := os.Open(CsvPath + config.CsvFilename)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		reader := csv.NewReader(file)
		records, err := reader.ReadAll()
		if err != nil {
			panic(err)
		}

		api.PostTweet(getStatus(config, records), nil)
	}
}

func getStatus(config Config, records [][]string) string {
	var record []string
	if config.Type == "seq" {
		record = getSequentialRecord(config, records)
	} else if config.Type == "random" {
		record = getRandomRecord(config, records)
	} else {
		panic(errors.New(fmt.Sprintf("invalid type: %s bot_name: %s", config.Type, config.Name)))
	}

	var columnKeys = strings.Split(config.StatusColumns, ",")
	columns := make([]interface{}, len(columnKeys))
	for i, keyString := range columnKeys {
		key, _ := strconv.Atoi(keyString)
		columns[i] = record[key]
	}

	var status = fmt.Sprintf(config.StatusFormat, columns...)
	if TweetMaxLength <= utf8.RuneCountInString(status) {
		return getStatus(config, records)
	} else {
		return status
	}
}

func getRandomRecord(config Config, records [][]string) []string {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	defer c.Close()
	rand.Seed(time.Now().UnixNano())
	for _, i := range rand.Perm(len(records)) {
		var key = fmt.Sprintf("%s%d", config.Name, i)
		used, err := redis.Bool(c.Do("GET", key))
		if err != nil || used != true {
			c.Do("SET", key, true)
			return records[i]
		}
	}
	for i := 0; i < len(records); i++ {
		var key = fmt.Sprintf("%s%d", config.Name, i)
		c.Do("SET", key, false)
	}
	return getRandomRecord(config, records)
}

func getSequentialRecord(config Config, records [][]string) []string {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	defer c.Close()
	for i := 0; i < len(records); i++ {
		var key = fmt.Sprintf("%s%d", config.Name, i)
		used, err := redis.Bool(c.Do("GET", key))
		if err != nil || used != true {
			c.Do("SET", key, true)
			return records[i]
		}
	}
	for i := 0; i < len(records); i++ {
		var key = fmt.Sprintf("%s%d", config.Name, i)
		c.Do("SET", key, false)
	}
	return getSequentialRecord(config, records)
}
