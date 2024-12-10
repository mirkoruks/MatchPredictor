create_basic_rec <- function(df) {
  
  basic_rec <- recipe(home_result ~ ., data = df) %>% 
    update_role(match_id, new_role = "ID") %>% 
    update_role(date, new_role = "index") %>% 
    step_naomit(all_predictors()) %>% 
    step_corr(all_numeric_predictors(), threshold = 0.9) %>% 
    step_zv(all_numeric_predictors()) %>% 
    step_normalize(all_numeric_predictors()) %>% 
    step_pca(all_numeric_predictors(), num_comp = tune())
  

  return(basic_rec)
}