package main

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jamiealquiza/tachymeter"
	_ "github.com/lib/pq"
)

var connStr = "postgres://localhost/connections-test?sslmode=disable"
var numLoops = 1000
var numTables = 50

func main() {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(fmt.Errorf("Error opening database: %v", err))
	}
	db.SetConnMaxLifetime(time.Duration(-1))
	db.SetMaxOpenConns(numLoops)
	db.SetMaxIdleConns(numLoops)

	fmt.Printf("\n")
	fmt.Printf("Establishing connections\n")
	fmt.Printf("------------------------\n")

	conns := make([]*sql.Conn, numLoops)
	for i := 0; i < numLoops; i++ {
		conns[i], err = establishConnection(db)
		if err != nil {
			panic(fmt.Errorf("Error establishing connections: %v", err))
		}
	}

	defer func() {
		fmt.Printf("\n")
		fmt.Printf("Closing connections\n")
		fmt.Printf("-------------------\n")

		for i := 0; i < numLoops; i++ {
			err := conns[i].Close()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error closing connection: %v\n", err)
			}
		}
	}()

	fmt.Printf("\n")
	fmt.Printf("Creating tables\n")
	fmt.Printf("---------------\n")

	for i := 0; i < numTables; i++ {
		_, err = conns[i].ExecContext(context.TODO(), fmt.Sprintf(`
		CREATE TABLE "users_%v" (
			id BIGSERIAL,
			name VARCHAR(50)
		)`,
			i))
		if err != nil {
			panic(fmt.Errorf("Error creating table: %v", err))
		}
	}

	defer func() {
		fmt.Printf("\n")
		fmt.Printf("Dropping tables\n")
		fmt.Printf("---------------\n")

		for i := 0; i < numTables; i++ {
			_, err = conns[i].ExecContext(context.TODO(), fmt.Sprintf(`
			DROP TABLE "users_%v"
		`, i))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error dropping table: %v\n", err)
			}

		}
	}()

	fmt.Printf("\n")
	fmt.Printf("Running warmup\n")
	fmt.Printf("--------------\n")

	// Do a couple initial runs to warm things up
	for i := 0; i < numLoops; i++ {
		_, err := run(conns[i], i)
		if err != nil {
			panic(err)
		}
	}

	fmt.Printf("\n")
	fmt.Printf("Running warmup\n")
	fmt.Printf("Running benchmark\n")
	fmt.Printf("-----------------\n")

	fmt.Fprintf(os.Stderr, "# connections,p50,p75,p95\n")

	for i := 0; i < numLoops; i++ {
		err := conns[i].Close()
		if err != nil {
			panic(err)
		}
		conns[i], err = establishConnection(db)
		if err != nil {
			panic(err)
		}

		var wg sync.WaitGroup
		t := tachymeter.New(&tachymeter.Config{Size: i + 1})
		var numErrors int32

		wg.Add(i + 1)

		// Start as many Goroutines as i + 1. We'll have only one on the first
		// loop, two on the second, three on the third, etc.
		for j := 0; j < i+1; j++ {
			conn := conns[j]
			go func() {
				defer wg.Done()

				elapsed, err := run(conn, i)
				if err != nil {
					atomic.AddInt32(&numErrors, 1)
					fmt.Fprintf(os.Stderr, "Error during work loop: %v\n", err)
				} else {
					t.AddTime(elapsed)
				}
			}()
		}

		wg.Wait()

		metrics := t.Calc()

		fmt.Printf("loop %v\n", i+1)
		fmt.Printf("-------\n")
		fmt.Printf("num errors = %v\n", numErrors)
		fmt.Printf("\n")
		fmt.Println(metrics.String())
		fmt.Printf("\n")
		fmt.Printf("\n")

		fmt.Fprintf(os.Stderr, "%v,%v,%v,%v\n",
			i+1,
			metrics.Time.P50.Seconds(),
			metrics.Time.P75.Seconds(),
			metrics.Time.P95.Seconds())
	}

	err = db.Close()
	if err != nil {
		panic(fmt.Errorf("Error closing database: %v", err))
	}
}

const connRetries = 5

func establishConnection(db *sql.DB) (*sql.Conn, error) {
	// Especially at higher parallelism, Postgres seems to have a lot of
	// trouble giving us a connection. If we couldn't acquire one, retry a
	// couple times with a backoff.
	for i := 0; i < connRetries; i++ {
		conn, err := db.Conn(context.TODO())

		if err == nil {
			return conn, nil
		}

		if i == connRetries-1 {
			return nil, fmt.Errorf("Error opening connection: %v", err)
		}

		var sleepTime float64
		for j := 0; j < i+1; j++ {
			sleepTime += rand.Float64()
		}

		// Convert seconds to nanoseconds by * 10**9
		time.Sleep(time.Duration(sleepTime * math.Pow(10, 9)))
	}

	panic("Unreachable")
}

func run(conn *sql.Conn, workerNum int) (time.Duration, error) {
	tableNum := workerNum % numTables

	start := time.Now()

	tx, err := conn.BeginTx(context.TODO(), nil)
	if err != nil {
		return time.Duration(0), fmt.Errorf("Error beginning transaction: %v", err)
	}

	ids := make([]int64, 10)

	for i := 0; i < 10; i++ {
		name := uuid.New().String()

		err := tx.QueryRow(fmt.Sprintf(`
			INSERT INTO "users_%v" (
				name
			) VALUES (
				$1
			) RETURNING id`,
			tableNum), name).Scan(&ids[i])
		if err != nil {
			return time.Duration(0), fmt.Errorf("Error inserting row: %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		var id int64
		var name string
		err := tx.QueryRow(fmt.Sprintf(`
			SELECT * FROM "users_%v"
			WHERE id = $1
		`, tableNum), ids[i]).Scan(&id, &name)
		if err != nil {
			return time.Duration(0), fmt.Errorf("Error selecting row: %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		_, err := tx.Exec(fmt.Sprintf(`
			DELETE FROM "users_%v"
			WHERE id = $1
		`, tableNum), ids[i])
		if err != nil {
			return time.Duration(0), fmt.Errorf("Error deleting row: %v", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return time.Duration(0), fmt.Errorf("Error committing transaction: %v", err)
	}

	elapsed := time.Now().Sub(start)
	return elapsed, nil
}
