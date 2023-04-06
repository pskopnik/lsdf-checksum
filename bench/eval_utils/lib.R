library(dplyr)
library(ggplot2)
library(lubridate)
library(readr)
library(scales)

# For shorter auto-legend
fct <- as.factor

format_bytes <- label_bytes(units="auto_binary", accuracy=0.1)

format_si <- label_number(accuracy=0.1, scale_cut=cut_short_scale())

format_bytes_per_second <- function(.x) {
	.x %>% format_bytes() %>% paste(., "/s", sep="")
}

format_dhms <- function(x) {
	format(as.period(seconds(x), unit="day"))
}

format_wdhms <- function(x) {
	p <- as.period(seconds(x), unit="day")

	w <- floor(day(p) / 7)

	# TODO: drop 0S if >0H or >0M

	paste(
		if_else(w >= 1, paste(w, "w", sep=""), ""),
		if_else(w >= 1, if_else(p - weeks(w) > 0, format(p - weeks(w)), ""), format(p))
	)
}

byte_breaks <- function(...) {
	# TODO: different procedure if range > 10?
	# TODO: different procedure if range > 256?

	# Standard, uses 2^i (powers of two)
	# TODO: should 5 be in here (10^i is included anyway)?
	standard_breaks <- extended_breaks(Q=c(1, 2, 4, 8, 5), ...)
	# Small, uses 2^-i
	# Use when breaks with tick distance < 1 ?B are displayed
	small_breaks <- extended_breaks(Q=c(1, 0.5, 0.25, 0.125), ...)

	function(limits) {
		range <- limits[2] - limits[1]
		scaling_exp <- floor(log(range, base=1024))
		scaling_factor <- 1024 ^ scaling_exp

		breaks <- standard_breaks(limits / scaling_factor)
		if (length(breaks) > 1 && breaks[2] - breaks[1] < 1) {
			breaks <- small_breaks(limits / scaling_factor)
		}

		return(
			breaks * scaling_factor
		)
	}
}

scale_x_bytes <- function(...) {
	scale_x_continuous(
		breaks = byte_breaks(),
		labels = format_bytes,
		...
	)
}

scale_x_byte_rate <- function(...) {
	scale_x_continuous(
		breaks = byte_breaks(),
		labels = format_bytes_per_second,
		...
	)
}

scale_y_bytes <- function(...) {
	scale_y_continuous(
		breaks = byte_breaks(),
		labels = format_bytes,
		...
	)
}

scale_y_byte_rate <- function(...) {
	scale_y_continuous(
		breaks = byte_breaks(),
		labels = format_bytes_per_second,
		...
	)
}
