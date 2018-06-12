library(readr)
library(dplyr)
library(ggplot2)

data <- read_delim(
	"results.tsv",
	" ",
	col_names=c("inserts", "transaction_size", "duration")
)

data_summary <- data %>%
	group_by(transaction_size) %>%
	summarize(mean_duration=mean(duration))

print(data_summary)

ggplot(data_summary, aes(x=transaction_size, y=mean_duration)) +
	geom_line() +
	geom_point()
ggsave("line_plot.pdf", width=9.05, height=6.99)

ggplot(data %>% filter(transaction_size < 11000)) +
	geom_boxplot(aes(x=transaction_size, group=transaction_size, y=duration))
ggsave("boxplot.pdf", width=9.05, height=6.99)

ggplot(data %>% mutate(transaction_size = factor(transaction_size))) +
	geom_boxplot(aes(x=transaction_size, group=transaction_size, y=duration))
ggsave("boxplot_factor.pdf", width=9.05, height=6.99)
