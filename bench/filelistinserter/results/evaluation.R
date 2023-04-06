library(readr)
library(dplyr)
library(ggplot2)
source("../../eval_utils/lib.R")

function() {
	source("./evaluation.R")

	d <- read_csv("results.csv")

	d %>%
		ggplot(aes(x=concurrency, y=duration, colour=fct(rows_per_stmt), group=interaction(stmts_per_transaction, rows_per_stmt))) +
			geom_line() +
			geom_point() +
			ylim(0, NA)
	ggsave("against_concurrency.svg", width=6, height=5)

	d_summarised <- d %>%
		# Aggregate multiple executions by calculating the mean
		group_by(concurrency, rows_per_stmt, stmts_per_transaction) %>%
		summarise(duration=mean(duration))

	d_summarised %>%
		filter(concurrency == 5) %>%
		ggplot(aes(x=rows_per_stmt, y=duration, colour=fct(stmts_per_transaction), group=interaction(stmts_per_transaction, concurrency))) +
			geom_line() +
			geom_point() +
			ylim(0, NA)
	ggsave("c=5.against_rows_per_stmt.svg", width=6, height=5)

	d_summarised %>%
		filter(concurrency == 5) %>%
		ggplot(aes(x=stmts_per_transaction, y=duration, colour=fct(rows_per_stmt), group=interaction(concurrency, rows_per_stmt))) +
			geom_line() +
			geom_point() +
			ylim(0, NA)
}
