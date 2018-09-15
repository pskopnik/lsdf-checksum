library(tibble)
library(dplyr)
library(purrr)
library(ggplot2)


simulate <- function(worker_num, interv, deviation_n, step_num=100000) {
	source("simulation.R")

	number_of_workers <<- worker_num
	maintaining_base_interval <<- interv
	number_of_steps <<- step_num
	maintaining_deviation_n <<- deviation_n

	run()

	hist <- tibble(
		time = hist_time,
		queue_length = hist_queue_length,
		threshold = hist_threshold,
		interval = hist_interval,
		exhausted = hist_exhausted,
		consumption = hist_consumption,
		consumption_ewma = hist_consumption_ewma,
		deviation = hist_deviation,
		deviation_ewma = hist_deviation_ewma,
		deviation_factor = hist_deviation_factor,
		deviation_factor_ewma = hist_deviation_factor_ewma,
		enqueued = hist_enqueued
	)

	hist %>%
		summarise(max=max(consumption)) %>%
		.$max %>%
		print

	return(
		hist
	)
}

plot_time_series <- function(data, upper_limit=NA, ewma_factor=1) {
	return(
		ggplot(data) +
			geom_line(aes(x=time, y=consumption)) +
			geom_line(aes(x=time, y=queue_length), colour="steelblue") +
			# geom_line(aes(x=time, y=consumption_ewma*ewma_factor), colour="green") +
			# geom_line(aes(x=time, y=deviation_ewma*ewma_factor), colour="violet") +
			# geom_line(aes(x=time, y=queue_length+enqueued), colour="grey") +
			geom_vline(data = data %>% filter(exhausted == TRUE), mapping = aes(xintercept=time), colour="red") +
			ylim(0, upper_limit)
	)
}

# Calculates an upper bound of inefficiency (fraction of time during which the workers were not fully utilised)
calc_inefficiency <- function(data) {
	return(
		(
			hist %>%
				mutate(time_span_width = time - lag(time)) %>%
				filter(exhausted == TRUE) %>%
				summarise(total_time = sum(time_span_width, na.rm=TRUE)) %>%
				.$total_time
		) / (
			hist %>%
				summarise(max = max(time)) %>%
				.$max
		)
	)
}

plot_sample_exhaustion <- function(hist, second_factor=1) {
	sample_time <- hist %>%
		filter(exhausted == TRUE) %>%
		sample_n(1) %>%
		.$time

	return(
		hist %>%
			filter(between(time, sample_time - 100*1000*second_factor, sample_time + 10*1000*second_factor)) %>%
			plot_time_series(ewma_factor=10) +
				geom_line(aes(x=time, y=threshold), colour="gray")
	)
}

function() {
	hist <- compile_hist()

	calc_inefficiency(hist)

	plot_time_series(hist)
}

function() {
	# These should never match any rows
	hist %>% filter(lag(queue_length)+enqueued < 12) %>% select(time, queue_length, threshold, enqueued, consumption, consumption_ewma)
	hist %>% filter(queue_length+consumption < 12) %>% select(time, queue_length, threshold, enqueued, consumption, consumption_ewma)
	hist %>% filter(queue_length+lead(enqueued) < threshold) %>% select(time, queue_length, threshold, enqueued, consumption, consumption_ewma)
}

function() {
	calc_inef <- partial(simulate, worker_num=50, deviation_n=5)

	intervals <- ceiling(qexp((1:40)/41) * 40000)

	inefficiencies <- vector("logical", length(intervals))

	for (a in 1:length(intervals)) {
		inefficiencies[a] <- calc_inef(intervals[a]) %>% calc_inefficiency
		print(paste("Finished", intervals[a], "with", inefficiencies[a]))
	}

	data <- tibble(
		test = "interval",
		worker_num = 50,
		deviation_n = 5,
		interval = intervals,
		inefficiency = inefficiencies
	)
}

function() {
	calc_inef <- partial(simulate, interv=1000, deviation_n=5)

	worker_nums <- unique(c(
		seq(1, 5, by=1),
		seq(5, 50, by=5),
		seq(50, 100, by=10),
		seq(100, 200, by=20),
		seq(200, 500, by=50),
		seq(500, 1000, by=100)
	))
	# Or
	# worker_nums <- exp(
	# 	1.1220 + 0.1761 * (1:35)
	# )

	inefficiencies <- vector("logical", length(worker_nums))

	for (a in 1:length(worker_nums)) {
		inefficiencies[a] <- calc_inef(worker_nums[a]) %>% calc_inefficiency
		print(paste("Finished", worker_nums[a], "with", inefficiencies[a]))
	}

	data <- tibble(
		test = "worker_num",
		worker_num = worker_nums,
		deviation_n = 5,
		interval = 1000,
		inefficiency = inefficiencies
	)
}

function() {
	calc_inef <- partial(simulate, interv=1000, worker_num=50)

	deviation_ns <- unique(c(
		seq(1, 10, by=1),
		seq(10, 20, by=2),
		seq(20, 40, by=4)
	))

	inefficiencies <- vector("logical", length(deviation_ns))

	for (a in 1:length(deviation_ns)) {
		inefficiencies[a] <- calc_inef(deviation_ns[a]) %>% calc_inefficiency
		print(paste("Finished", deviation_ns[a], "with", inefficiencies[a]))
	}

	data <- tibble(
		test = "deviation_n",
		worker_num = 50,
		deviation_n = deviation_ns,
		interval = 1000,
		inefficiency = inefficiencies
	)
}
