package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/olekukonko/tablewriter"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"log"
	"os"
	"reflect"
	"sort"
	"strconv"
)

const (
	sourceHost     = "localhost"
	sourcePort     = 5455
	sourceUser     = "postgresUser"
	sourcePassword = "postgresPW"
	sourceDbName   = "postgresDB"
	targetHost     = "localhost"
	targetPort     = 5455
	targetUser     = "postgresUser"
	targetPassword = "postgresPW"
	targetDbName   = "postgresDBNew"
)

type PostgresConnection struct {
	conn *pgx.Conn
}

func Connect(connectionString string) *PostgresConnection {
	conn, err := pgx.Connect(context.Background(), connectionString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	return &PostgresConnection{conn}
}

func (c *PostgresConnection) Close() {
	defer c.conn.Close(context.Background())
}

func main() {
	log.SetFlags(0)
	sourceConnectionString := fmt.Sprintf("postgres://%v:%v@%v:%v/%v", sourceUser, sourcePassword, sourceHost, sourcePort, sourceDbName)
	targetConnectionString := fmt.Sprintf("postgres://%v:%v@%v:%v/%v", targetUser, targetPassword, targetHost, targetPort, targetDbName)
	sourceConnection := Connect(sourceConnectionString)
	targetConnection := Connect(targetConnectionString)

	defer sourceConnection.Close()
	defer targetConnection.Close()

	sTableNames := sourceConnection.getBaseTableNames()
	tTableNames := targetConnection.getBaseTableNames()
	mergedNames := merge(sTableNames, tTableNames)
	if len(sTableNames) != len(mergedNames) || len(tTableNames) != len(mergedNames) {
		sTableNames := sourceConnection.getBaseTableNames()
		tTableNames := targetConnection.getBaseTableNames()

		sort.Strings(mergedNames)
		data := make(map[int][]string)
		for i, name := range mergedNames {
			row := []string{name, mapChar(slices.Contains(sTableNames, name), "X", nil), mapChar(slices.Contains(tTableNames, name), "X", nil)}
			data[i] = row
		}
		log.Println("* CHECK THAT TABLE EXISTS")
		printTableFromString([]string{"Table Name", "Source", "Target"}, maps.Values(data))
		//log.Fatalf("There is a differnece between the tables count, Source: %v / Target: %v", sTableCount, tTableCount)
	}

	columnNames := []string{"name", "Source RowCount", "Target RowCount", "Source CheckSum", "Target CheckSum"}
	sourceTablesCount := sourceConnection.countRowCountForTables()
	targetTablesCount := targetConnection.countRowCountForTables()
	data := make(map[int][]string)
	log.Println("* CHECK TABLE CONTENT")
	for i, name := range mergedNames {

		sourceTableCount := sourceTablesCount[name]
		targetTableCount := targetTablesCount[name]
		var sourceTableCheckSum, targetTableCheckSum, sameData string
		if sourceTableCount > 0 {
			sourceTableCheckSum = sourceConnection.checksumForDataInTable(name)
		}
		if targetTableCount > 0 {
			targetTableCheckSum = targetConnection.checksumForDataInTable(name)
		}

		if sourceTableCount > 0 || targetTableCount > 0 {
			if sourceTableCheckSum == targetTableCheckSum {
				sameData = "IDENTICAL"
			} else {
				sameData = "MISMATCH"
			}
		}
		row := []string{name, strconv.FormatInt(sourceTableCount, 10), strconv.FormatInt(targetTableCount, 10), sourceTableCheckSum, targetTableCheckSum, sameData}
		data[i] = row
	}
	printTableFromString(columnNames, maps.Values(data))

	// CHECK ROW BASED TABLE DIFF
	tableName := "apis"
	sourceConnection.checksumForRowDataInTable(tableName, "id")
}

func printTableFromString(columnNames []string, data [][]string) {

	table := tablewriter.NewWriter(log.Writer())
	table.SetHeader(columnNames)
	for _, v := range data {
		table.Append(v)
	}
	table.Render()
}

func printTable(columnNames []string, items [][]interface{}) {

	data := make(map[int][]string)

	for i, row := range items {
		var tableRow []string
		for _, cell := range row {
			tableRow = append(tableRow, fmt.Sprintf("%v", cell))
		}
		data[i] = tableRow
	}

	table := tablewriter.NewWriter(log.Writer())
	table.SetHeader(columnNames)
	for _, v := range data {
		table.Append(v)
	}
	table.Render()
}

func (c *PostgresConnection) countBaseTables() int {
	ctx := context.Background()
	var counter int
	q := "select count(*) from information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';"
	err := c.conn.QueryRow(ctx, q).Scan(&counter)
	if err != nil {
		log.Fatalf("Unexpected error for Query: %v", err)
	}
	return counter
}

func (c *PostgresConnection) getBaseTableNames() []string {
	ctx := context.Background()
	q := "select table_name from information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';"
	rows, err := c.conn.Query(ctx, q)
	if err != nil {
		log.Fatalf("Unexpected error for Query: %v", err)
	}

	var outputRows []string
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			log.Fatalf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, fmt.Sprint(row[0]))
	}
	return outputRows
}

func (c PostgresConnection) countRowCountForTables() map[string]int64 {
	ctx := context.Background()
	q := "SELECT relname,n_live_tup FROM pg_stat_user_tables ORDER BY relname ASC;"
	rows, err := c.conn.Query(ctx, q)
	if err != nil {
		log.Fatalf("Unexpected error for Query: %v", err)
	}
	outputRows := make(map[string]int64)
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			log.Fatalf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows[fmt.Sprint(row[0])] = reflect.ValueOf(row[1]).Int()
	}
	return outputRows
}

func (c PostgresConnection) checksumForRowDataInTable(tableName string, primaryKey string) map[string]string {
	ctx := context.Background()
	q := fmt.Sprintf("SELECT %v, md5(textin(record_out(%v.*))) as hash FROM %v", primaryKey, tableName)
	rows, err := c.conn.Query(ctx, q)
	if err != nil {
		log.Fatalf("Unexpected error for Query: %v", err)
	}
	outputRows := make(map[string]string)
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			log.Fatalf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows[fmt.Sprint(row[0])] = reflect.ValueOf(row[1]).String()
	}
	return outputRows
}

func (c PostgresConnection) checksumForDataInTable(tableName string) string {
	ctx := context.Background()
	q := fmt.Sprintf("SELECT %v, md5(array_to_string(array_agg(hash), '')) FROM (SELECT 1 as groupid, md5(textin(record_out(%v.*))) as hash FROM %v ORDER BY id) agg GROUP BY groupid", tableName, tableName)
	var checksum string
	row := c.conn.QueryRow(ctx, q)

	err := row.Scan(&checksum)
	if err != nil && err != pgx.ErrNoRows {
		log.Fatalf("Unexpected error for Query2: %v", err)
	}

	return checksum
}

// difference returns the elements in `a` that aren't in `b`.
func difference(a, b []string) []string {
	mb := make(map[string]struct{}, len(b))
	for _, x := range b {
		mb[x] = struct{}{}
	}
	var diff []string
	for _, x := range a {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}

func contains(a, b []string) []string {
	mb := make(map[string]struct{}, len(b))
	for _, x := range b {
		mb[x] = struct{}{}
	}
	var diff []string
	for _, x := range a {
		if _, found := mb[x]; found {
			diff = append(diff, x)
		}
	}
	return diff
}

func merge(a []string, b []string) []string {

	check := make(map[string]int)
	d := append(a, b...)
	res := make([]string, 0)
	for _, val := range d {
		check[val] = 1
	}

	for letter, _ := range check {
		res = append(res, letter)
	}

	return res
}

func mapChar(condition bool, char string, fallBack *string) string {
	if condition {
		return char
	}

	if fallBack != nil {
		return *fallBack
	}
	return ""
}
