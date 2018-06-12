library(readr)
library(dplyr)
library(ggplot2)

data <- read_delim(
	"results.tsv",
	" ",
	col_names=c("bytes_read", "read_size", "duration")
)

data_summary <- data %>%
	group_by(read_size) %>%
	summarize(mean_duration=mean(duration))

print(data_summary)

ggplot(data_summary, aes(x=read_size, y=mean_duration)) +
	geom_line() +
	geom_point()
ggsave("line_plot.pdf", width=9.05, height=6.99)

ggplot(data %>% filter(read_size < 36000)) +
	geom_boxplot(aes(x=read_size, group=read_size, y=duration))
ggsave("boxplot.pdf", width=9.05, height=6.99)

ggplot(data %>% mutate(read_size = factor(read_size))) +
	geom_boxplot(aes(x=read_size, group=read_size, y=duration))
ggsave("boxplot_factor.pdf", width=9.05, height=6.99)
