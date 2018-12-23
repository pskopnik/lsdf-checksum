library(readr)
library(dplyr)
library(ggplot2)
library(stringr)

function() {
	source("evaluation.R")

	d  <- read_csv("bench_results.csv")

	d_sum <- d %>%
		group_by(case, scenario, operation) %>%
		summarise(
			med_dur = median(duration),
			mean_dur = mean(duration),
			qu95_dur = quantile(duration, probs=0.95)
		) %>%
		ungroup() %>%
		arrange(med_dur)

	num_rows <- nrow(d_sum)

	d_sum %>%
		print(n=num_rows)

	d_sum %>%
		filter(operation == "update_existing") %>%
		print(n=num_rows)

	d_sum %>%
		filter(case %in% c("TestCaseDontUpdateRand", "TestCaseDontUpdateRandUpdateAllConditionsInOn")) %>%
		print()

	d %>%
		filter(case %in% c("TestCaseDontUpdateRand", "TestCaseDontUpdateRandUpdateAllConditionsInOn")) %>%
		ggplot() +
			geom_boxplot(aes(x=case, y=duration)) +
			facet_grid(operation ~ scenario) +
			scale_x_discrete(labels = function(x) str_wrap(gsub('([[:upper:]])', ' \\1', x), width = 15))

	# Filtered: Only update_existing operation

	case_levels <- d_sum %>%
		filter(scenario == "no_inserts" & operation == "update_existing") %>%
		arrange(med_dur) %>%
		pull(case)
	d %>%
		mutate(case = factor(case, levels = case_levels, ordered = TRUE)) %>%
		filter(scenario == "no_inserts") %>%
		ggplot() +
			geom_boxplot(aes(x=case, y=duration)) +
			facet_grid(operation ~ .) +
			scale_x_discrete(labels = function(x) str_wrap(gsub('([[:upper:]])', ' \\1', x), width = 15))
	ggsave("scenario_no_inserts.pdf")

	plot_cases(d, "no_inserts", "insert_new")
	ggsave("scenario_no_inserts_operation_insert_new.pdf")
	plot_cases(d, "no_inserts", "delete_old")
	ggsave("scenario_no_inserts_operation_delete_old.pdf")
	plot_cases(d, "no_inserts", "update_existing")
	ggsave("scenario_no_inserts_operation_update_existing.pdf")
}

plot_cases <- function(.data, scenario_name, operation_name) {
	case_levels <- .data %>%
		filter(scenario == scenario_name & operation == operation_name) %>%
		group_by(case) %>%
		summarise(
			med_dur = median(duration),
			mean_dur = mean(duration),
			qu95_dur = quantile(duration, probs=0.95)
		) %>%
		ungroup() %>%
		arrange(med_dur) %>%
		pull(case)

	return(
		.data %>%
			mutate(case = factor(case, levels = case_levels, ordered = TRUE)) %>%
			filter(scenario == scenario_name & operation == operation_name) %>%
			ggplot() +
				geom_boxplot(aes(x=case, y=duration)) +
				facet_grid(operation ~ .) +
				scale_x_discrete(labels = function(x) str_wrap(gsub('([[:upper:]])', ' \\1', x), width = 15))
	)
}
