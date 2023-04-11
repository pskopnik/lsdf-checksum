library(readr)
library(dplyr)
library(ggplot2)
source("../../eval_utils/lib.R")

function() {
	source("./evaluation.R")

	d <- read_csv("results.csv")

	d_summarised <- d %>%
		# Aggregate multiple executions by calculating the mean
		group_by(env, concurrency, rows_per_stmt, stmts_per_transaction) %>%
		summarise(duration=mean(duration))

	d %>%
		filter(env == "default_config") %>%
		ggplot(aes(x=concurrency, y=duration, colour=fct(rows_per_stmt), group=interaction(env, stmts_per_transaction, rows_per_stmt))) +
			geom_line() +
			geom_point() +
			ylim(0, NA)
	ggsave("env=default_config.against_concurrency.svg", width=6, height=5)

	d_summarised %>%
		filter(env == "default_config") %>%
		filter(concurrency == 5) %>%
		ggplot(aes(x=rows_per_stmt, y=duration, colour=fct(stmts_per_transaction), group=interaction(env, stmts_per_transaction, concurrency))) +
			geom_line() +
			geom_point() +
			ylim(0, NA)
	ggsave("env=default_config.c=5.against_rows_per_stmt.svg", width=6, height=5)

	d_summarised %>%
		filter(env == "default_config") %>%
		filter(concurrency == 5) %>%
		ggplot(aes(x=stmts_per_transaction, y=duration, colour=fct(rows_per_stmt), group=interaction(env, concurrency, rows_per_stmt))) +
			geom_line() +
			geom_point() +
			ylim(0, NA)

	d_summarised %>%
		filter(env == "5GiB_buffer") %>%
		filter(concurrency == 5) %>%
		ggplot(aes(x=rows_per_stmt, y=duration, colour=fct(stmts_per_transaction), group=interaction(env, stmts_per_transaction, concurrency))) +
			geom_line() +
			geom_point() +
			ylim(0, NA)
	ggsave("env=5GiB_buffer.c=5.against_rows_per_stmt.svg", width=6, height=5)

	d_summarised %>%
		filter(env == "5GiB_buffer") %>%
		filter(concurrency == 5) %>%
		ggplot(aes(x=stmts_per_transaction, y=duration, colour=fct(rows_per_stmt), group=interaction(env, concurrency, rows_per_stmt))) +
			geom_line() +
			geom_point() +
			ylim(0, NA)

}
