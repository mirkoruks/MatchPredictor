rm(list = ls())

library(tidymodels)
library(tidyverse)
library(doParallel)
library(discrim)
library(agua)
library(colino)
library(corrr)

df <- read_csv("data/modeldata.csv")
df <- df %>% 
  mutate(home_result = factor(home_result)) %>% 
  mutate(date = as.POSIXct(date, format = "%Y-%-m-%d")) %>% 
  select(-ends_with(c("mean10","mean15"))) %>% 
  filter(if_all(everything(), ~ !is.na(.x))) %>% 
  filter(if_all(everything(), ~ !is.infinite(.x))) %>% 
  arrange(date)


# Data splitting
df_split <- initial_time_split(df,
                               prop = 0.8)

df_train <- training(df_split)

vars <- df_train %>% 
  mutate(home_result2 = case_when(home_result == "W" ~ 2,
                                  home_result == "D" ~ 1,
                                  home_result == "L" ~ 0,
                                  TRUE ~ NA)) %>% 
  select(-home_result) %>% 
  correlate() %>% 
  focus(home_result2) %>% 
  arrange(desc(home_result2)) %>% 
  filter(row_number() <= 50)


df_test <- testing(df_split)

roll_cv_all <- rolling_origin(df_train,
                              initial = round((nrow(df_train)/20) * 8),
                              assess = round((nrow(df_train)/20) * 3),
                              skip = round((nrow(df_train)/20))-1,
                              cumulative = FALSE)


# Preprocessing recipes
basic_rec <- recipe(home_result ~ ., data = df_train) %>% 
  update_role(match_id, new_role = "ID") %>% 
  update_role(date, new_role = "index") %>% 
  step_naomit(all_predictors()) %>% 
  step_corr(all_numeric_predictors(), threshold = 0.9) %>% 
  step_zv(all_numeric_predictors()) %>% 
  step_normalize(all_numeric_predictors()) 

boruta_rec <- basic_rec %>% 
  colino::step_select_boruta(all_predictors(), outcome = "home_result")

pca_rec <- basic_rec %>% 
  step_pca(all_numeric_predictors(), num_comp = tune())

pca_param <- parameters(num_comp(range = c(0,100)))

# KNN
knn_spec <- nearest_neighbor(
  mode = "classification",
  engine = "kknn",
  neighbors = tune()) 

knn_param <- parameters(neighbors(range = c(1,50)))

# Multinomial regression
multinom_spec <- multinom_reg(
  mode = "classification",
  engine = "glmnet",
  penalty = tune(),
  mixture = 1)

multinom_param <- parameters(penalty(range = c(0.001,1), trans = NULL))

# Linear Discriminant Analysis
lda_spec <- discrim_linear(
  mode = "classification",
  engine = "MASS")


# Quadratic Discriminant Analysis
qda_spec <- discrim_quad(
  mode = "classification",
  engine = "MASS")

# Naive Bayes
nbayes_spec <- naive_Bayes(
  mode = "classification",
  Laplace = 1,
  engine = "h2o")

# Random Forests
forest_spec <- rand_forest(
  mode = "classification",
  engine = "ranger",
  trees = 1000,
  min_n = tune(),
  mtry = tune())

forest_param <- parameters(mtry(range = c(1L, floor(ncol(df_train)/3))),
                           min_n(range = c(2L,40L))) 

# Create workflow sets
boruta_wf <- workflow_set(  
  preproc = list("basic" = boruta_rec),
  models = list(#"knn" = knn_spec,
                #"multinomial" = multinom_spec,
                "forest" = forest_spec)) %>%
  #option_add(param_info = knn_param, id = "basic_knn") %>% 
  #option_add(param_info = multinom_param, id = "basic_multinomial") %>% 
  option_add(param_info = forest_param, id = "basic_forest") 

pca_wf <- workflow_set(
  preproc = list("pca" = pca_rec),
  models = list("knn" = knn_spec,
                "multinomial" = multinom_spec,
                "lda" = lda_spec,
                "qda" = qda_spec,
                "nbayes" = nbayes_spec)) %>% 
  option_add(param_info = pca_param, id = NULL) %>%
  option_add(param_info = knn_param, id = "pca_knn") %>% 
  option_add(param_info = multinom_param, id = "pca_multinomial") 

final_wf <- bind_rows(basic_wf, pca_wf)

# knn_grid <- mean10_wf %>% 
#   extract_parameter_set_dials(id = "mean10_knn") %>% 
#   grid_regular(levels = 15)
# 
# multinom_grid <- mean10_wf %>% 
#   extract_parameter_set_dials(id = "mean10_multinomial") %>% 
#   grid_regular(levels = 15)
# 
# mean10_grid_wf <- mean10_wf %>% 
#   option_add(grid = knn_grid, id = "mean10_knn") %>% 
#   option_add(grid = multinom_grid, id = "mean10_multinomial") 

# final_wf %>% extract_parameter_dials("num_comp", id = "pca_nbayes")
# final_wf %>% extract_parameter_dials("mtry", id = "basic_forest")
# final_wf %>% extract_parameter_dials("num_comp", id = "pca_knn")
# final_wf %>% extract_parameter_dials("penalty", id = "pca_multinomial")
boruta_wf %>% extract_parameter_dials("min_n", id = "basic_forest")
boruta_wf %>% extract_parameter_dials("mtry", id = "basic_forest")

filename <- "log.txt"
if (file.exists(filename)) {
  #Delete file if it exists
  file.remove(filename)
}
cl <- makeCluster(10, outfile = filename)
registerDoParallel(cl)


# ctrl_grid<- control_grid(save_pred = TRUE,
#                          parallel_over = "everything",
#                          save_workflow = TRUE)
# 
# grid_results <- mean10_grid_wf %>%
#   workflow_map(
#     seed = 1,
#     resamples = roll_cv_all,
#     control = ctrl_grid,
#     verbose = TRUE
#   )

ctrl_bayes<- control_bayes(save_pred = TRUE,
                           parallel_over = "resamples",
                           save_workflow = TRUE)

# rf_fit <- workflow() %>% 
#   add_recipe(basic_rec) %>% 
#   add_model(forest_spec) %>% 
#   tune_grid(
#     #seed = 1,
#     resamples = roll_cv_all,
#     grid = fores_grid,
#     #iter = 30,
#     #verbose = TRUE,
#     control = ctrl_grid
#   )


bayes_results <- boruta_wf %>%
  workflow_map(
    seed = 1,
    fn = "tune_bayes",
    resamples = roll_cv_all,
    initial = 15,
    iter = 60,
    verbose = TRUE,
    control = ctrl_bayes
  )


stopCluster(cl)
stopImplicitCluster()


bayes_results %>% 
  workflowsets::rank_results(rank_metric = "accuracy") %>% 
  View()
  select(model, .config, accuracy = mean, rank)


bayes_results %>% 
  extract_workflow_set_result("basic_multinomial") %>%
  show_best(metric = "accuracy")

bayes_results %>% 
  extract_workflow_set_result("basic_forest") %>%
  show_best(metric = "accuracy")


bayes_results %>% 
  extract_workflow_set_result("basic_forest") %>%
  autoplot(type = "performance")




autoplot(
  bayes_results,
  rank_metric = "accuracy",  # <- how to order models
  metric = "accuracy",       # <- which metric to visualize
  select_best = TRUE     # <- one point per workflow
) #+
  geom_text(aes(y = mean - 1/2, label = wflow_id), angle = 90, hjust = 1) +
  lims(y = c(3.5, 9.5)) +
  theme(legend.position = "none")








bayes_results 
#ctrl_grid <- control_grid(save_pred = TRUE,
#                          save_workflow = TRUE,
#                          parallel_over = "resamples")
# Grid tuning
#grid_results <-
#  basic_all_wf %>%
#  workflow_map(
#    seed = 1,
#    resamples = roll_cv_all,
#    control = ctrl_grid,
#    verbose = TRUE
#  )


#%>% 
#ggplot(aes(x = .panel_x, y = .panel_y)) + 
#  geom_point() +
#  geom_blank() +
#  facet_matrix(vars(num_comp, penalty), layer.diag = 2) + 
#  labs(title = "Latin Hypercube design with 20 candidates")


#grid_results <- basic_all_wf %>% 
#  workflow_map(seed = 1,
#               fn = "tune_grid",
#               resamples = roll_cv_all)


