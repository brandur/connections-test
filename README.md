# connections-test

A simple benchmark to demonstrate the degradation of
performance in Postgres as its total number of active
connections increases.

Run with:

    dropdb connections-test ; createdb connections-test && go run main.go

Information is logged to stdout and comma-separated data
points are sent to stderr, so after it looks like it's
working, you may want to collect that data with something
like:

    dropdb connections-test ; createdb connections-test && go run main.go 2> results.csv
