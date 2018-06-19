library(purrr)

# Note: All times are in milliseconds
# Note: This simulation models enqueuing as taking negligible time.

# Simulation Parameters

number_of_workers <- 1
# Time for processing task = task_size * work_time_factor
work_size_time_factor <- 1/1000
work_size_distribution <- function(n) { rlnorm(n, meanlog=11, sdlog=3) }
number_of_steps <- 100000

# Scheduler Parameters

# During start up phase: threshold <- number_of_workers * start_up_treshold_n
start_up_threshold_n <- 5
start_up_interval <- 10
start_up_steps <- 1000
# During maintaining phase: threshold <- expected consumption * maintaining_threshold_n
maintaining_threshold_n <- 1
maintaining_deviation_n <- 10
maintaining_base_interval <- 10000
deviation_alpha <- 0.1
deviation_factor_alpha <- 0.1
# "Significant" lifetime of consumption values, used in calculating the EWMA
consumption_lifetime <- 10 * 1000
# Minimum threshold parameters
min_threshold <- 2
min_worker_threshold <- 1/4

# State Variables

start_up_step_count <- 0
queue_length <- 0
threshold <- 0
interval <- 0
consumption_ewma <- 0
deviation_ewma <- 0
deviation_factor_ewma <- 1
previous_queue_length <- 0
enqueued <- 0
phase <- ""

# History / Log

exhausted <- 0
consumption <- 0
deviation <- 0
deviation_factor <- 0

i <- 1
time <- 0

hist_time <- vector("integer", number_of_steps + 1)
hist_queue_length <- vector("integer", number_of_steps + 1)
hist_threshold <- vector("integer", number_of_steps + 1)
hist_interval <- vector("integer", number_of_steps + 1)
hist_exhausted <- vector("logical", number_of_steps + 1)
hist_consumption <- vector("integer", number_of_steps + 1)
hist_consumption_ewma <- vector("integer", number_of_steps + 1)
hist_deviation <- vector("integer", number_of_steps + 1)
hist_deviation_ewma <- vector("integer", number_of_steps + 1)
hist_deviation_factor <- vector("integer", number_of_steps + 1)
hist_deviation_factor_ewma <- vector("integer", number_of_steps + 1)
hist_enqueued <- vector("integer", number_of_steps + 1)

# Simulation State

workers_finished <- vector("integer", 0)
# Initialisation moved to init
#workers_finished <- vector("integer", number_of_workers)

# Simulation

init <- function() {
	workers_finished <<- vector("integer", number_of_workers)

	phase <<- "start_up"
	interval <<- start_up_interval
	threshold <<- number_of_workers * start_up_threshold_n

	consumption_ewma <<- 0
	deviation_ewma <<- 0
	deviation_factor_ewma <<- 1
	previous_queue_length <<- 0
	enqueued <<- 0

	record()
}

consume_tasks <<- function() {
	while (TRUE) {
		workers_finished <<- sort(workers_finished)

		num <- length(head_while(workers_finished, ~ .x <= interval))
		num <- min(num, queue_length)
		if (num <= 0) {
			break
		}
		workers_finished[1:num] <<- workers_finished[1:num] + (work_size_distribution(num) * work_size_time_factor)
		queue_length <<- queue_length - num
	}

	workers_finished <<- workers_finished - interval
}

# Probes queue and schedules the enqueueing of new tasks.
probe_queue <- function() {
	if (queue_length == 0) {
		exhausted <<- TRUE
	} else {
		exhausted <<- FALSE
	}

	consumption <<- previous_queue_length + enqueued - queue_length

	deviation <<- abs(consumption - (consumption_ewma * interval / 1000))

	alpha <- 1 - exp(-interval / consumption_lifetime)
	consumption_ewma <<- (1 - alpha) * consumption_ewma + alpha * (consumption * 1000 / interval)

	alpha <- deviation_alpha
	deviation_ewma <<- (1 - alpha) * deviation_ewma + alpha * (deviation * 1000 / interval)

	if (consumption_ewma != 0) {
		deviation_factor <<- consumption / (consumption_ewma * interval / 1000)

		alpha <- deviation_factor_alpha
		deviation_factor_ewma <<- (1 - alpha) * deviation_factor_ewma + alpha * deviation_factor
	} else {
		deviation_factor <<- NA
	}

	if (exhausted) {
		threshold <<- 2 * threshold
	} else if (phase == "start_up") {
		start_up_step_count <<- start_up_step_count + 1

		if (start_up_step_count >= start_up_steps) {
			phase <<- "maintaining"
			interval <<- maintaining_base_interval
			threshold <<- max(
				maintaining_threshold_n * (maintaining_deviation_n * ceiling(deviation_ewma * interval / 1000) + ceiling(consumption_ewma * interval / 1000)),
				min_threshold,
				ceiling(min_worker_threshold * number_of_workers)
			)
			deviation_factor_ewma <- 1
		}
	} else if (phase == "maintaining") {
		# if (deviation_factor_ewma >= 2 || deviation_factor_ewma <= 0.5) {
		# 	interval <<- interval * 1/deviation_factor_ewma
		# 	deviation_factor_ewma <<- 1
		# }

		threshold <<- max(
			maintaining_threshold_n * (maintaining_deviation_n * ceiling(deviation_ewma * interval / 1000) + ceiling(consumption_ewma * interval / 1000)),
			min_threshold,
			ceiling(min_worker_threshold * number_of_workers)
		)
	}
}

enqueue <- function() {
	previous_queue_length <<- queue_length
	queue_length <<- max(threshold, queue_length)
	enqueued <<- queue_length - previous_queue_length
}

record <- function() {
	hist_time[[i]] <<- time

	hist_queue_length[[i]] <<- queue_length
	hist_threshold[[i]] <<- threshold
	hist_interval[[i]] <<- interval
	hist_exhausted[[i]] <<- exhausted
	hist_consumption[[i]] <<- consumption
	hist_consumption_ewma[[i]] <<- consumption_ewma
	hist_deviation[[i]] <<- deviation
	hist_deviation_ewma[[i]] <<- deviation_ewma
	hist_deviation_factor[[i]] <<- deviation_factor
	hist_deviation_factor_ewma[[i]] <<- deviation_factor_ewma
	hist_enqueued[[i]] <<- enqueued

	time <<- time + interval
	i <<- i + 1
}

do_step <- function() {
	enqueue()
	consume_tasks()
	probe_queue()
	record()
}


run <- function() {
	init()

	for (i_anon in 1:number_of_steps) {
		do_step()
	}
}

compile_hist <- function() {
	library(tibble)

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

	return(hist)
}

function() {
	run()

	hist <- compile_hist()
}

