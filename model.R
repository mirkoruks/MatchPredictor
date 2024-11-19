rm(list = ls())

library(tidymodels)
library(tidyverse)

source("model_helpers.R")

df <- read_csv("data/modeldata.csv")
df <- df %>% 
  mutate(home_result = factor(home_result)) %>% 
  mutate(date = as.Date(date, "%Y-%-m-%d")) %>% 
  arrange(date)



df_split <- initial_time_split(df,
                               prop = 0.8)
df_train <- training(df_split)
df_test <- testing(df_split)

basic_all_rec <- create_basic_rec(df)
basic5_rec <- create_basic_rec(df %>% select(-ends_with(c("mean10", "mean15"))))
basic10_rec <- create_basic_rec(df %>% select(-ends_with(c("mean5", "mean15"))))
basic15_rec <- create_basic_rec(df %>% select(-ends_with(c("mean10", "mean5"))))

# KNN
knn_spec <- nearest_neighbor() %>% 
  set_engine("knn") %>% 
  set_mode("classification")

# Multinomial regression
multinom_spec <- multinom_reg() %>% 
  set_engine("multinom_reg") %>% 
  set_mode("classification")

# Linear Discriminant Analysis
lda_spec <- discrim_linear() %>% 
  set_engine("MASS") %>% 
  set_mode("classification")

# Quadratic Discriminant Analysis
qda_spec <- discrim_quad() %>% 
  set_engine("MASS") %>% 
  set_mode("classification")

# Naive Bayes
nbayes_spec <- naive_Bayes() %>% 
  set_engine("h2o") %>% 
  set_mode("classification")

# Bagging
bagging_spec <- bag_tree() %>% 
  set_engine("rpart") %>% 
  set_mode("classification")

# Random Forests
forest_spec <- rand_forest() %>% 
  set_engine("ranger") %>% 
  set_mode("classification")

# Gradient Boosting
boosting_spec <- boost_tree() %>% 
  set_engine("xgboost") %>% 
  set_mode("classification")

# BART
bart_spec <- bart() %>% 
  set_engine("dbarts") %>% 
  set_mode("classification")

# Support Vector Machines

# Neural networks
neural_spec <- mlp() %>% 
  set_engine("brulee") %>% 
  set_mode("classification")

  