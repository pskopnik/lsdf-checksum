#!/bin/bash

cmd="./filelistinserter"
config_template="./filelistinserter.config.yaml.tpl"
sleep_time="180"

function run() {
	local config_template="$1"
	local param_rowsperstmt="$2"
	local param_stmtspertransaction="$3"
	local param_concurrency="$4"

	# Create config file and replace parameter placeholders
	config="$(mktemp)"
	cp "$config_template" "$config"
	sed -i 's/\$ROWSPERSTMT\$/'"$param_rowsperstmt"/ "$config"
	sed -i 's/\$STMTSPERTRANSACTION\$/'"$param_stmtspertransaction"/ "$config"
	sed -i 's/\$CONCURRENCY\$/'"$param_concurrency"/ "$config"

	echo "$(date -Is) : Starting filelistinserter" >&2
	echo "$(date -Is) : Params rowsperstmt=$param_rowsperstmt stmtspertransaction=$param_stmtspertransaction concurrency=$param_concurrency" >&2
	# Store duration in seconds while forwarding stdout and stderr to stderr
	duration=$(LC_NUMERIC=C TIMEFORMAT='%R'; { time "$cmd" "$config" 2>&1; } 3>&2 2>&1 >&3)
	echo "$(date -Is) : Finished filelistinserter, took $duration secs" >&2

	rm "$config"

	echo "$param_rowsperstmt,$param_stmtspertransaction,$param_concurrency,$duration"
}

function print_header() {
	echo "rows_per_stmt,stmts_per_transaction,concurrency,duration"
}

function exploratory() {
	run "$config_template"  2000   1  1
	sleep "$sleep_time"
	run "$config_template"  2000   2  1
	sleep "$sleep_time"
	run "$config_template"  2000   4  1
	sleep "$sleep_time"
	run "$config_template"  2000   8  1
	sleep "$sleep_time"
	run "$config_template"  2000  16  1
	sleep "$sleep_time"
	run "$config_template"  2000  32  1
	sleep "$sleep_time"
	run "$config_template"  2000  64  1

	sleep "$sleep_time" && sleep "$sleep_time"

	run "$config_template"  2000   1  1
	sleep "$sleep_time"
	run "$config_template"  2000   1  5
	sleep "$sleep_time"
	run "$config_template"  2000   1 10
	sleep "$sleep_time"
	run "$config_template"  2000   1 20
	sleep "$sleep_time"
	run "$config_template"  2000   1 40
	sleep "$sleep_time"
	run "$config_template"  2000   1 80

	sleep "$sleep_time" && sleep "$sleep_time"

	run "$config_template"   500   1  1
	sleep "$sleep_time"
	run "$config_template"  1000   1  1
	sleep "$sleep_time"
	run "$config_template"  2000   1  1
	sleep "$sleep_time"
	run "$config_template"  4000   1  1
	sleep "$sleep_time"
	run "$config_template"  8000   1  1
	sleep "$sleep_time"
	run "$config_template" 16000   1  1
}

function concurrency_5() {
	run "$config_template"  2000   1  5
	sleep "$sleep_time"
	run "$config_template"  2000   2  5
	sleep "$sleep_time"
	run "$config_template"  2000   4  5
	sleep "$sleep_time"
	run "$config_template"  2000   8  5
	sleep "$sleep_time"
	run "$config_template"  2000  16  5
	sleep "$sleep_time"
	run "$config_template"  2000  32  5
	sleep "$sleep_time"
	run "$config_template"  2000  64  5

	sleep "$sleep_time" && sleep "$sleep_time"

	run "$config_template"   500   1  5
	sleep "$sleep_time"
	run "$config_template"  1000   1  5
	sleep "$sleep_time"
	run "$config_template"  2000   1  5
	sleep "$sleep_time"
	run "$config_template"  4000   1  5
	sleep "$sleep_time"
	run "$config_template"  8000   1  5
	sleep "$sleep_time"
	run "$config_template" 16000   1  5
	sleep "$sleep_time"
}

print_header

exploratory

sleep "$sleep_time" && sleep "$sleep_time" && sleep "$sleep_time" && sleep "$sleep_time"

concurrency_5
