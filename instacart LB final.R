library(data.table)
library(dplyr)
library(tidyr)
# set.seed(3260)


path <- "D:/Docs/Instacart"

setwd("D:/Docs/Instacart")

aisles <- fread(file.path(path, "aisles.csv"))
departments <- fread(file.path(path, "departments.csv"))
orderp <- fread(file.path(path, "order_products__prior.csv"))
ordert <- fread(file.path(path, "order_products__train.csv"))
orders <- fread(file.path(path, "orders.csv"))
products <- read.csv("products.csv", header = TRUE, stringsAsFactors = FALSE)


# Reshape data ------------------------------------------------------------
aisles$aisle <- as.factor(aisles$aisle)
departments$department <- as.factor(departments$department)
orders$eval_set <- as.factor(orders$eval_set)
products$product_name <- as.factor(products$product_name)

products <- products %>% 
  inner_join(aisles) %>% inner_join(departments) %>% 
  select(-aisle_id, -department_id)
rm(aisles, departments)

ordert$user_id <- orders$user_id[match(ordert$order_id, orders$order_id)]

orders_products <- orders %>% inner_join(orderp, by = "order_id")

# orders_products <- orders_products %>% filter(order_number > 1)

# first_orders <- orders_products %>% group_by(user_id) %>% summarise(first_order_id = min(order_id))
# 
# orders_products <- orders_products %>% filter(!(order_id %in% first_orders$first_order_id))

rm(orderp,first_orders)
gc()


# Products ----------------------------------------------------------------
prd <- orders_products %>%
  arrange(user_id, order_number, product_id) %>%
  group_by(user_id, product_id) %>%
  mutate(product_time = row_number()) %>%
  ungroup() %>%
  group_by(product_id) %>%
  summarise(
    prod_orders = n(),
    prod_distinct_user = n_distinct(user_id),
    prod_distinct_ord = n_distinct(order_id),
    prod_reorders = sum(reordered),
    prod_first_orders = sum(product_time == 1),
    prod_second_orders = sum(product_time == 2),
    prod_add_to_cart = round(mean(add_to_cart_order,na.rm = TRUE)),
    prod_mean_dow = round(mean(order_dow, na.rm = TRUE)),
    prod_mean_hour = round(mean(order_hour_of_day, na.rm = TRUE)),
    prod_mean_order = round(mean(order_id, na.rm = TRUE)),
    prod_recent_order = round(max(order_id,na.rm = TRUE))
  )

prd$prod_reorder_probability <- prd$prod_second_orders / prd$prod_first_orders
prd$prod_mean_order <- prd$prod_mean_order/max(orders_products$order_id)
prd$prod_recent_order <- prd$prod_recent_order/max(orders_products$order_id)
# the below 2 lines i am not sure which one i used
# prd$prod_reorder_times <- 1 + prd$prod_reorders / prd$prod_first_orders
prd$prod_reorder_times <- prd$prod_reorders / prd$prod_first_orders

prd$prod_reorder_ratio <- prd$prod_reorders / prd$prod_orders
prd$prod_reorder_quotient <- prd$prod_reorder_ratio * prd$prod_reorders

prd <- prd %>% select(-prod_reorders, -prod_first_orders, -prod_second_orders)

rm(products)
gc()

# Users -------------------------------------------------------------------
users <- orders %>%
  filter(eval_set == "prior") %>%
  group_by(user_id) %>%
  summarise(
    user_orders = max(order_number),
    user_period = sum(days_since_prior_order, na.rm = T),
    user_mean_days_since_prior = mean(days_since_prior_order, na.rm = T),
    user_mean_order = max(order_id,na.rm = TRUE),
    user_mean_dow = round(mean(order_dow, na.rm = TRUE)),
    user_mean_hour = round(mean(order_hour_of_day, na.rm = TRUE))
  )

us <- orders_products %>%
  group_by(user_id) %>%
  summarise(
    user_total_products = n(),
    user_distinct_prod = n_distinct(product_id),
    user_reorder_ratio = sum(reordered == 1) / sum(order_number > 1),
    user_add_to_cart = round(mean(add_to_cart_order,na.rm = TRUE)),
    user_distinct_products = n_distinct(product_id),
    user_order_sum = sum(order_number)
  )

users$user_mean_order <- users$user_mean_order/max(orders_products$order_id)

users <- users %>% inner_join(us)
users$user_average_basket <- users$user_total_products / users$user_orders

us <- orders %>%
  filter(eval_set != "prior") %>%
  select(user_id, order_id, eval_set,
         time_since_last_order = days_since_prior_order)

users <- users %>% inner_join(us)

rm(us)
gc()


# Database ----------------------------------------------------------------
data <- orders_products %>%
  group_by(user_id, product_id) %>% 
  summarise(
    up_orders = n(),
    up_first_order = min(order_number),
    up_last_order = max(order_number),
    up_average_cart_position = mean(add_to_cart_order),
    prod_user_mean_days_since_prior = mean(days_since_prior_order, na.rm = TRUE))

recent_reord <- orders_products %>%
  filter(reordered == 1) %>%
  group_by(user_id, product_id) %>% 
  summarise(
    sum_order = sum(order_number))

user_first <- orders_products %>% 
              filter(order_number > 1 & reordered != 1) %>%
              group_by(user_id) %>%
              summarise(
                first_orders = n()
              )

rm(orders_products, orders)

data <- data %>% 
  inner_join(prd, by = "product_id") %>%
  inner_join(users, by = "user_id")

data <- data %>%
  left_join(recent_reord, by = c("user_id","product_id"))

data <- data %>% left_join(user_first, by = c("user_id"))

data$first_orders[is.na(data$first_orders)] <- 0
data$first_orders <- ifelse(data$user_orders > 1, data$first_orders/(data$user_orders - 1), 0)
data$sum_order[is.na(data$sum_order)] <- 0

data$up_order_rate <- data$up_orders / data$user_orders
data$prod_user_mean_days_since_prior <- (data$prod_user_mean_days_since_prior/data$user_period) * data$user_orders
data$prod_user_mean_days_since_prior[is.na(data$prod_user_mean_days_since_prior), ] <- 0
data$up_orders_since_last_order <- data$user_orders - data$up_last_order
# the below 2 lines i am not sure which one i used
data$up_order_rate_since_first_order <- ifelse((data$user_orders - data$up_first_order) == 0, 0,data$up_orders / (data$user_orders - data$up_first_order))
# data$up_order_rate_since_first_order <- data$up_orders / (data$user_orders - data$up_first_order + 1)
data$recent_ratio <- data$sum_order/data$up_orders
data$prod_user_recent <- data$up_last_order/data$user_orders
data$user_prod_recent_quotient <- data$sum_order/data$user_order_sum
data$user_product_priority <- data$up_average_cart_position/data$up_last_order
# data$user_prop <- data$up_orders * data$up_order_rate

data$sum_order <- NULL

data <- data %>% 
  left_join(ordert %>% select(user_id, product_id, reordered), 
            by = c("user_id", "product_id"))

rm(ordert, prd, users, recent_reord)
gc()

products <- read.csv("products.csv", header = TRUE, stringsAsFactors = FALSE)

data <- data %>% inner_join(products) %>% select(-product_name)

aisle_order <- data %>% 
  filter(eval_set %in% c("train","prior")) %>% 
  group_by(aisle_id) %>% 
  summarise(total = sum(as.numeric(prod_orders)), 
            reordered = sum(as.numeric(prod_orders * prod_reorder_ratio)),
            aisle_users = n_distinct(user_id),
            aisle_ordercnt = n_distinct(order_id))

dept_order <- data %>% 
  filter(eval_set %in% c("train","prior")) %>% 
  group_by(department_id) %>% 
  summarise(total = sum(as.numeric(prod_orders)), 
            reordered = sum(as.numeric(prod_orders * prod_reorder_ratio)),
            dept_users = n_distinct(user_id),
            dept_ordercnt = n_distinct(order_id))

user_aisle <- data %>%
  group_by(user_id, aisle_id) %>%
  summarise(
    aisle_products = n()
  ) %>% ungroup() %>%
  group_by(user_id) %>% 
  filter(aisle_products == max(aisle_products)) %>% 
  select(user_id,aisle_id)

user_aisle <- user_aisle[!duplicated(user_aisle$user_id),]
colnames(user_aisle) <- c("user_id","user_fav_aisle")

user_dept <- data %>%
  group_by(user_id, department_id) %>%
  summarise(
    dept_products = n()
  ) %>% ungroup() %>%
  group_by(user_id) %>% 
  filter(dept_products == max(dept_products)) %>% 
  select(user_id, department_id)

user_dept <- user_dept[!duplicated(user_dept$user_id),]
colnames(user_dept) <- c("user_id","user_fav_dept")

rm(products)
gc()
dept_order$dept_reorder_ratio <- dept_order$reordered/dept_order$total
dept_order$total <- NULL
dept_order$reordered <- NULL

aisle_order$aisle_reorder_ratio <- aisle_order$reordered/aisle_order$total
aisle_order$total <- NULL
aisle_order$reordered <- NULL

data <- data %>% inner_join(aisle_order)
data <- data %>% inner_join(dept_order)
data$aisle_id <- NULL
data$department_id <- NULL
data <- data %>% inner_join(user_aisle)
data <- data %>% inner_join(user_dept)
rm(products,aisle_order,dept_order,user_aisle,user_dept)
gc()

# write.csv(data, file = "data.csv", row.names = F)


# Train / Test datasets ---------------------------------------------------
data$user_mean_hour <- NULL
data$user_mean_dow <- NULL
data$prod_mean_hour <- NULL
data$prod_mean_dow <- NULL
data$prod_distinct_user <- NULL
data$dept_users <- NULL
data$user_fav_dept <- NULL
data$aisle_users <- NULL
data$user_distinct_prod <- NULL
data$prod_mean_order <- NULL
data$user_total_products <- NULL
data$user_mean_order <- NULL
data$aisle_ordercnt <- NULL
data$user_fav_aisle <- NULL
data$dept_ordercnt <- NULL
data$prod_distinct_ord <- NULL
data$prod_orders <- NULL
data$user_distinct_products <- NULL
# data$prod_user_recent <- NULL
data$prod_recent_order <- NULL

# data$time_since_last_order <- NULL
# data$user_period <- NULL

train <- as.data.frame(data[data$eval_set == "train",])
train$eval_set <- NULL
train$user_id <- NULL
train$product_id <- NULL
train$order_id <- NULL
train$reordered[is.na(train$reordered)] <- 0

test <- as.data.frame(data[data$eval_set == "test",])
test$eval_set <- NULL
test$user_id <- NULL
test$reordered <- NULL

rm(data)
gc()

# Model -------------------------------------------------------------------
library(xgboost)
library(dplyr)

set.seed(4351)

params <- list(
  "objective"           = "reg:linear",
  "eval_metric"         = "logloss",
  "eta"                 = 0.05,
  "max_depth"           = 8,
  "min_child_weight"    = 26,
  "gamma"               = 0.16,
  "subsample"           = 0.72,
  "colsample_bytree"    = 0.68,
  "alpha"               = 2e-05,
  "lambda"              = 10
)
# library(caret)
# intrain = createDataPartition(train$reordered,p=0.6,list=FALSE)
# subtrain <- train[intrain, ]
# validDF <- train[-intrain, ]
# subtrain[,"prod_user_mean_days_since_prior"][is.na(subtrain[,"prod_user_mean_days_since_prior"])] <- 0
# X <- xgb.DMatrix(as.matrix(subtrain %>% select(-reordered)), label = subtrain$reordered)
# model <- xgboost(data = X, params = params, nrounds = 80)
# 
# importance <- xgb.importance(colnames(X), model = model)
# xgb.ggplot.importance(importance)
# 
# X <- xgb.DMatrix(as.matrix(validDF %>% select(-reordered)))
# validDF$pred_reorder <- predict(model, X)
# validDF$pred_reorderxgb <- (validDF$pred_reorder > 0.19) * 1
# 
# confusionMatrix(validDF$pred_reorderxgb,validDF$reordered)


# Apply model -------------------------------------------------------------
X <- xgb.DMatrix(as.matrix(train %>% select(-reordered)), label = train$reordered)
model <- xgboost(data = X, params = params, nrounds = 400)
importance <- xgb.importance(colnames(X), model = model)

Y <- xgb.DMatrix(as.matrix(test %>% select(-order_id, -product_id)))
test$reordered <- predict(model, Y)

# Author: Faron, Lukasz Grad
#
# Quite fast implementation of Faron's expected F1 maximization using Rcpp and R
library(inline)
library(Rcpp)
Sys.setenv("PKG_CXXFLAGS"="-std=c++11")

# Input: p: item reorder probabilities (sorted), p_none: none probability (0 if not specified)
# Output: matrix[2][n + 1] out: out[0][j] - F1 score with top j products and None
#                               out[1][j] - F1 score with top j products
cppFunction(
  'NumericMatrix get_expectations(NumericVector p, double p_none) {
  // Assuming p is sorted, p_none == 0 if not specified
  int n = p.size();
  NumericMatrix expectations = NumericMatrix(2, n + 1);
  double DP_C[n + 2][n + 1];
  std::fill(DP_C[0], DP_C[0] + (n + 2) * (n + 1), 0);
  if (p_none == 0.0) {
  p_none = std::accumulate(p.begin(), p.end(), 1.0, [](double &a, double &b) {return a * (1.0 - b);});
  }
  DP_C[0][0] = 1.0;
  for (int j = 1; j < n; ++j)
  DP_C[0][j] = (1.0 - p[j - 1]) * DP_C[0][j - 1];
  for (int i = 1; i < n + 1; ++i) {
  DP_C[i][i] = DP_C[i - 1][i - 1] * p[i - 1];
  for (int j = i + 1; j < n + 1; ++j)
  DP_C[i][j] = p[j - 1] * DP_C[i - 1][j - 1] + (1.0 - p[j - 1]) * DP_C[i][j - 1];
  }
  double DP_S[2 * n + 1];
  double DP_SNone[2 * n + 1];
  for (int i = 1; i < (2 * n + 1); ++i) {
  DP_S[i] = 1.0 / (1.0 * i);
  DP_SNone[i] = 1.0 / (1.0 * i + 1);
  }
  for (int k = n; k >= 0; --k) {
  double f1 = 0.0;
  double f1None = 0.0;
  for (int k1 = 0; k1 < (n + 1); ++k1) {
  f1 += 2 * k1 * DP_C[k1][k] * DP_S[k + k1];
  f1None += 2 * k1 * DP_C[k1][k] * DP_SNone[k + k1];
  }
  for (int i = 1; i < (2 * k - 1); ++i) {
  DP_S[i] = (1 - p[k - 1]) * DP_S[i] + p[k - 1] * DP_S[i + 1];
  DP_SNone[i] = (1 - p[k - 1]) * DP_SNone[i] + p[k - 1] * DP_SNone[i + 1];
  }
  expectations(0, k) = f1None + 2 * p_none / (2.0 + k);
  expectations(1, k) = f1;
  }
  return expectations;
  }'
)

# Input: ps - item reorder probabilities, prods - item ids
# Output: reordered items string (as required in submission)
exact_F1_max_none <- function(ps, prods) {
  prods <- as.character(prods)
  perm <- order(ps, decreasing = T)
  ps <- ps[perm]
  prods <- prods[perm]
  expectations <-  get_expectations(ps, 0.0)
  max_idx <-  which.max(expectations)
  add_none <- max_idx %% 2 == 1
  size <- as.integer(max(0, max_idx - 1) / 2)
  if (size == 0) {
    return("None")
  }
  else {
    if (add_none)
      return(paste(c(prods[1:size], "None"), collapse = " "))
    else 
      return(paste(prods[1:size], collapse = " "))
  }
}

# How to use it with dplyr:
#
submission <- test %>%
  group_by(order_id) %>%
  summarise(products = exact_F1_max_none(reordered, product_id))


write.csv(submission, file = "submit.csv", row.names = F)

