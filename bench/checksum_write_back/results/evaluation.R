library(readr)
library(dplyr)
library(ggplot2)
library(stringr)

function() {
	# Experiments

	source("evaluation.R")

	d <- read_results(
		# "prelim.csv"
		"sequential_explorative.csv"
		# "concurrent_explorative.csv"
		# "updatecase_explorative.csv"
	)

	d %>%
		group_by(method) %>%
		summarise(
			min = min(total_duration),
			max = max(total_duration),
			mean = mean(total_duration),
			qu05_dur = quantile(total_duration, probs=0.05)
		)

	d %>%
		group_by(method, concurrency) %>%
		top_n(-1, total_duration) %>%
		arrange(method, total_duration) %>%
		print(., n=nrow(.))

	d %>%
		filter(method == "updatecase" & batch_size == 500) %>%
		arrange(method, total_duration)

	d %>%
		filter(method == "updatecase" & batch_size > 1 & batch_size < 2000) %>%
		ggplot() +
			geom_point(aes(x=batch_size, y=total_duration, shape=as.factor(concurrency), col=transaction_size, group=interaction(concurrency, transaction_size))) +
			geom_line(aes(x=batch_size, y=total_duration, col=transaction_size, group=interaction(concurrency, transaction_size)))

	d %>%
		filter(concurrent == FALSE) %>%
		filter(method == "updatecase" & batch_size > 1) %>%
		ggplot() +
			geom_point(aes(x=batch_size, y=total_duration, col=as.factor(transaction_size), group=transaction_size)) +
			geom_line(aes(x=batch_size, y=total_duration, col=as.factor(transaction_size), group=transaction_size))

	# Batch Size

	d %>%
		# filter(concurrent == FALSE) %>%
		group_by(method, batch_size) %>%
		summarise(
			total_duration_mean = mean(total_duration),
			total_duration_median = median(total_duration),
			total_duration_min = min(total_duration),
			total_duration_max = max(total_duration),
			total_duration_005 = quantile(total_duration, probs=0.05)
		) %>%
		filter(batch_size > 1 & batch_size <= 1000) %>%
		ggplot() +
			geom_point(aes(x=batch_size, y=total_duration_min, col=method, group=method)) +
			geom_line(aes(x=batch_size, y=total_duration_min, col=method, group=method))

	# Transaction Size

	d %>%
		#filter(concurrent == FALSE) %>%
		group_by(method, transaction_size) %>%
		summarise(
			total_duration_mean = mean(total_duration),
			total_duration_median = median(total_duration),
			total_duration_min = min(total_duration),
			total_duration_max = max(total_duration),
			total_duration_005 = quantile(total_duration, probs=0.05)
		) %>%
		ggplot() +
			geom_point(aes(x=transaction_size, y=total_duration_min, col=method, group=method)) +
			geom_line(aes(x=transaction_size, y=total_duration_min, col=method, group=method))

	# Concurrency

	d %>%
		group_by(method, concurrency) %>%
		summarise(
			total_duration_mean = mean(total_duration),
			total_duration_median = median(total_duration),
			total_duration_min = min(total_duration),
			total_duration_max = max(total_duration),
			total_duration_005 = quantile(total_duration, probs=0.05)
		) %>%
		ggplot() +
			geom_point(aes(x=concurrency, y=total_duration_min, col=method, group=method)) +
			geom_line(aes(x=concurrency, y=total_duration_min, col=method, group=method))
}

function() {
	# Evalute explorative benchmarks including different concurrencies

	source("evaluation.R")

	d <- read_results(
		"concurrent_explorative.csv"
	)

	# What is the fastest method (best parameter combination): updatecase
	d %>%
		group_by(method) %>%
		summarise(
			min = min(total_duration),
			max = max(total_duration),
			mean = mean(total_duration),
			qu05_dur = quantile(total_duration, probs=0.05)
		) %>%
		arrange(min)

	# Best parameter combination for each method
	d %>%
		group_by(method) %>%
		top_n(-1, total_duration) %>%
		arrange(total_duration)

	# Best parameter combination for each (method, concurrency)
	d %>%
		group_by(method, concurrency) %>%
		top_n(-1, total_duration) %>%
		arrange(method, total_duration) %>%
		print(., n=nrow(.))

	# Batch Size

	d %>%
		group_by(method, batch_size) %>%
		summarise(
			total_duration_mean = mean(total_duration),
			total_duration_median = median(total_duration),
			total_duration_min = min(total_duration),
			total_duration_max = max(total_duration),
			total_duration_005 = quantile(total_duration, probs=0.05)
		) %>%
		ggplot() +
			geom_point(aes(x=batch_size, y=total_duration_min, col=method, group=method)) +
			geom_line(aes(x=batch_size, y=total_duration_min, col=method, group=method))
	ggsave("concurrent_batch_size.pdf", width=6, height=4)

	# Transaction Size

	d %>%
		group_by(method, transaction_size) %>%
		summarise(
			total_duration_mean = mean(total_duration),
			total_duration_median = median(total_duration),
			total_duration_min = min(total_duration),
			total_duration_max = max(total_duration),
			total_duration_005 = quantile(total_duration, probs=0.05)
		) %>%
		ggplot() +
			geom_point(aes(x=transaction_size, y=total_duration_min, col=method, group=method)) +
			geom_line(aes(x=transaction_size, y=total_duration_min, col=method, group=method))
	ggsave("concurrent_transaction_size.pdf", width=6, height=4)

	# Concurrency

	d %>%
		group_by(method, concurrency) %>%
		summarise(
			total_duration_mean = mean(total_duration),
			total_duration_median = median(total_duration),
			total_duration_min = min(total_duration),
			total_duration_max = max(total_duration),
			total_duration_005 = quantile(total_duration, probs=0.05)
		) %>%
		ggplot() +
			geom_point(aes(x=concurrency, y=total_duration_min, col=method, group=method)) +
			geom_line(aes(x=concurrency, y=total_duration_min, col=method, group=method))
	ggsave("concurrent_concurrency.pdf", width=6, height=4)
}

function() {
	# Evaluate updatecase explorative benchmarks

	source("evaluation.R")

	d <- read_results(
		"updatecase_explorative.csv"
	)

	d %>%
		group_by(method, concurrency) %>%
		top_n(-1, total_duration) %>%
		arrange(method, total_duration) %>%
		print(., n=nrow(.))

	# batch_size == 500 leads to the fastest total_duration
	d %>%
		filter(method == "updatecase" & batch_size == 500) %>%
		arrange(method, total_duration)

	# Plot transaction_size on the x-axis: Insignificant impact
	d %>%
		filter(method == "updatecase" & batch_size == 500) %>%
		ggplot() +
			geom_point(aes(x=transaction_size, y=total_duration, col=concurrency, group=concurrency)) +
			geom_line(aes(x=transaction_size, y=total_duration, col=concurrency, group=concurrency))

	# Plot concurrency on the x-axis: This shows huge improvements with increasing concurrency
	d %>%
		filter(method == "updatecase") %>%
		group_by(method, transaction_size, concurrency) %>%
		summarise(
			total_duration_mean = mean(total_duration),
			total_duration_median = median(total_duration),
			total_duration_min = min(total_duration),
			total_duration_max = max(total_duration),
			total_duration_005 = quantile(total_duration, probs=0.05)
		) %>%
		ggplot() +
			geom_point(aes(x=concurrency, y=total_duration_min, col=as.factor(transaction_size), group=transaction_size)) +
			geom_line(aes(x=concurrency, y=total_duration_min, col=as.factor(transaction_size), group=transaction_size))
	ggsave("updatecase_concurrency_transaction_size.pdf", width=6, height=4)

	# Plot batch_size on the x-axis, plot all concurrencies (group by concurrency)
	d %>%
		filter(method == "updatecase") %>%
		group_by(method, batch_size, concurrency) %>%
		summarise(
			total_duration_mean = mean(total_duration),
			total_duration_median = median(total_duration),
			total_duration_min = min(total_duration),
			total_duration_max = max(total_duration),
			total_duration_005 = quantile(total_duration, probs=0.05)
		) %>%
		ggplot() +
			geom_point(aes(x=batch_size, y=total_duration_min, col=as.factor(concurrency), group=concurrency)) +
			geom_line(aes(x=batch_size, y=total_duration_min, col=as.factor(concurrency), group=concurrency))
	# Same, but without batch_size == 1, this leads to a more readable plot
	d %>%
		filter(method == "updatecase" & batch_size > 1) %>%
		group_by(method, batch_size, concurrency) %>%
		summarise(
			total_duration_mean = mean(total_duration),
			total_duration_median = median(total_duration),
			total_duration_min = min(total_duration),
			total_duration_max = max(total_duration),
			total_duration_005 = quantile(total_duration, probs=0.05)
		) %>%
		ggplot() +
			geom_point(aes(x=batch_size, y=total_duration_min, col=as.factor(concurrency), group=concurrency)) +
			geom_line(aes(x=batch_size, y=total_duration_min, col=as.factor(concurrency), group=concurrency))
	ggsave("updatecase_batch_size_concurrency.pdf", width=6, height=4)
}

# Little use
plot_transaction_size_col_concurrency <- function(d, filter_method="basic") {
	my_breaks <- d %>%
		group_by(concurrency) %>%
		count() %>%
		pull(concurrency)
	return(
		d %>%
			filter(method == filter_method & transaction_size > 1) %>%
			ggplot() +
				geom_point(aes(x=transaction_size, y=total_duration, col=concurrency, group=concurrency)) +
				geom_line(aes(x=transaction_size, y=total_duration, col=concurrency, group=concurrency)) +
				scale_color_gradient(trans="log", breaks=my_breaks, labels=my_breaks)
	)
}

read_results <- function(path) {
	return(
		read_csv(
			path,
			col_types = cols(
				method = col_character(),
				batch_size = col_integer(),
				transaction_size = col_integer(),
				concurrent = col_logical(),
				concurrency = col_integer(),
				total_duration = col_double()
			)
		) %>%
			mutate(
				concurrency = if_else(concurrent == FALSE, as.integer(0), concurrency)
			)
	)
}
