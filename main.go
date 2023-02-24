package main

import (
	"database/sql"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	_ "github.com/go-sql-driver/mysql"
)

const (
	// Info messages
	Info = 1

	// Warning Messages
	Warning = 2

	// Error Messages
	Error = 3
)

var timeNow = time.Now()

type Table struct {
	TableName string
	RowCount  int
}

type Options struct {
	HostName                 string
	Bind                     string
	UserName                 string
	Password                 string
	Database                 string
	DatabaseRowCountTreshold int
	TableRowCountTreshold    int
	BatchSize                int
	ForceSplit               bool
	MySQLDumpPath            string
	OutputDirectory          string
	ExecutionStartDate       time.Time
	DailyRotation            int
	WeeklyRotation           int
	MonthlyRotation          int
}

const _MYSQL_DUMP_PATH_ = "usr/bin/mysqldump"

func main() {

	options := Options{
		HostName:                 "localhost",
		Bind:                     "3306",
		UserName:                 "admin",
		Password:                 "password",
		Database:                 "database-name",
		DatabaseRowCountTreshold: 10000000,
		TableRowCountTreshold:    5000000,
		BatchSize:                1000000,
		ForceSplit:               false,
		MySQLDumpPath:            "",
		OutputDirectory:          "",
		ExecutionStartDate:       timeNow,
		DailyRotation:            5,
		WeeklyRotation:           2,
		MonthlyRotation:          1,
	}

	if options.OutputDirectory == "" {
		dir, err := os.Getwd()
		checkError(err)
		options.OutputDirectory = dir
	}

	outputdir := options.OutputDirectory
	os.MkdirAll(outputdir+"/daily/"+timeNow.Format("2006-01-02"), os.ModePerm)
	os.MkdirAll(outputdir+"/weekly", os.ModePerm)
	os.MkdirAll(outputdir+"/monthly", os.ModePerm)

	if _, err := os.Stat(_MYSQL_DUMP_PATH_); os.IsNotExist(err) {
		printMessage("mysqldump binary can not be found, please specify correct value for mysqldump-path parameter", Error)
		os.Exit(1)
	}

	fmt.Println(options)

	tableList := getTableList(&options)
	fmt.Println(tableList)

	totalRowCount := getTotalRowCount(tableList)
	fmt.Println(totalRowCount)

	if !options.ForceSplit && totalRowCount <= options.DatabaseRowCountTreshold {
		// options.ForceSplit is false
		// and if total row count of a database is below defined threshold
		// then generate one file containing both schema and data
		printMessage(fmt.Sprintf("Condition:1 options.ForceSplit (%t) && totalRowCount (%d) <= options.DatabaseRowCountTreshold (%d)", options.ForceSplit, totalRowCount, options.DatabaseRowCountTreshold), Info)
		generateSingleFileDataBackup(&options)
	} else if options.ForceSplit && totalRowCount <= options.DatabaseRowCountTreshold {
		// options.ForceSplit is true
		// and if total row count of a database is below defined threshold
		// then generate two files one for schema, one for data
		printMessage(fmt.Sprintf("Condition:2 options.ForceSplit (%t) && totalRowCount (%d) <= options.DatabaseRowCountTreshold (%d)", options.ForceSplit, totalRowCount, options.DatabaseRowCountTreshold), Info)
	} else if totalRowCount > options.DatabaseRowCountTreshold {
		printMessage(fmt.Sprintf("Condition:3 options.ForceSplit (%t) && totalRowCount (%d) > options.DatabaseRowCountTreshold (%d)", options.ForceSplit, totalRowCount, options.DatabaseRowCountTreshold), Info)
	}

}

func getTableList(opt *Options) []Table {

	db, err := sql.Open("mysql", opt.UserName+":"+opt.Password+"@tcp("+opt.HostName+":"+opt.Bind+")/tedbul")
	checkError(err)

	defer db.Close()

	rows, err := db.Query("SELECT table_name as TableName, table_rows as RowCount FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + opt.Database + "'")
	checkError(err)

	var tables []Table
	for rows.Next() {
		var tableName string
		var rowCount int

		err = rows.Scan(&tableName, &rowCount)
		checkError(err)

		tables = append(tables, *NewTable(tableName, rowCount))
	}

	printMessage(strconv.Itoa(len(tables))+" tables retrived : "+opt.Database, Info)
	return tables
}

func NewTable(tableName string, rowCount int) *Table {
	return &Table{
		TableName: tableName,
		RowCount:  rowCount,
	}
}

func getTotalRowCount(tables []Table) int {
	result := 0
	for _, table := range tables {
		result += table.RowCount
	}
	return result
}

func generateSingleFileDataBackup(options *Options) {
	printMessage("Generating single file data backup : "+options.Database, Info)

	var args []string
	args = append(args, fmt.Sprintf("-h%s", options.HostName))
	args = append(args, fmt.Sprintf("-u%s", options.UserName))
	args = append(args, fmt.Sprintf("-p%s", options.Password))

	args = append(args, "--no-create-db")
	args = append(args, "--skip-triggers")
	args = append(args, "--no-create-info")

	timestamp := strings.Replace(strings.Replace(options.ExecutionStartDate.Format("2006-01-02"), "-", "", -1), ":", "", -1)
	filename := path.Join(options.OutputDirectory, "daily", timeNow.Format("2006-01-02"), options.Database+"-"+options.ExecutionStartDate.Format("2006-01-02"), fmt.Sprintf("%s_%s_%s.sql", options.Database, "DATA", timestamp))
	_ = os.Mkdir(path.Dir(filename), os.ModePerm)

	args = append(args, fmt.Sprintf("-r%s", filename))
	args = append(args, options.Database)

	printMessage("mysqldump is being executed with parameters : "+strings.Join(args, " "), Info)

	fmt.Println(args)
}

func printMessage(message string, messageType int) {
	colors := map[int]color.Attribute{Info: color.FgGreen, Warning: color.FgHiYellow, Error: color.FgHiRed}
	color.Set(colors[messageType])
	fmt.Println(message)
	color.Unset()
}

func checkError(err error) {
	if err != nil {
		color.Set(color.FgHiRed)
		panic(err)
	}
}
