library(readr)
library(ggplot2)
library(dplyr)

data <- read_csv("results.csv")

g <- ggplot(data %>% filter(test == "insert" & context == "empty"))
g + geom_boxplot(aes(x=method, y=duration)) + ggtitle("Duration for different INSERT methods. Scenario: Empty.")
ggsave("duration_insert_empty.pdf", width=7, height=9)

g <- ggplot(data %>% filter(test == "insert" & context == "no_inserts"))
g + geom_boxplot(aes(x=method, y=duration)) + ggtitle("Duration for different INSERT methods. Scenario: No inserts.")
ggsave("duration_insert_no_inserts.pdf", width=7, height=9)

g <- ggplot(data %>% filter(test == "update" & context == "no_inserts"))
g + geom_boxplot(aes(x=method, y=duration)) + ggtitle("Duration for different UPDATE methods. Scenario: No inserts.")
ggsave("duration_update_no_inserts.pdf", width=7, height=9)

g <- ggplot(data %>% filter(test == "delete" & context == "no_inserts"))
g + geom_boxplot(aes(x=method, y=duration)) + ggtitle("Duration for different DELETE methods. Scenario: No inserts.")
ggsave("duration_delete_no_inserts.pdf", width=7, height=9)
