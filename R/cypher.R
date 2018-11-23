
#' @export
call_cypher <- function (query, con)
{
  #query <- 'MATCH (n:Movie)\nRETURN n'
  #query <- 'MATCH (n) \nWHERE size(labels(n)) = 0\nRETURN n'
  #query <- "MATCH (n)\nRETURN ID(n) as `.id`,labels(n) as `.label`,n"

  include_stats <- TRUE
  query_clean <- clean_query(query)
  query_jsonised <- to_json_neo(query_clean)
  body <- glue("{\"statements\" : [ %query_jsonised% ]}",
               .open = "%", .close = "%")
  res <- POST(url = glue("{con$url}/db/data/transaction/commit?includeStats=true"),
              add_headers(.headers = c(`Content-Type` = "application/json",
                                       accept = "application/json",
                                       Authorization = paste0("Basic ",con$auth))),
              body = body)
  if(status_code(res) != 200) stop("API error")

  api_content <- content(res)
  if (length(api_content$errors) > 0) {
    return(list(error_code = api_content$errors[[1]]$code,
                error_message = api_content$errors[[1]]$message))
  }
  results <- api_content$results[[1]]
  if (length(results$data) == 0) {
    message("No data returned.")
    return()
  }
  res_names <- results$columns
  res_data <- results$data
  list(columns = res_names, data = transpose(res_data))
}


to_json_neo <- function (query) {
  jsonlite::toJSON(list(statement = query, includeStats = TRUE,
                        meta = TRUE, resultDataContents = list("row")), auto_unbox = TRUE)
}

clean_query <- function (query) {
  res <- gsub("^\\/\\/.+$", "\n", query, perl = TRUE)
  res <- gsub("\n", " ", res)
  res <- gsub("\"", "'", res)
  res
}


flatten_df_list <- function(x){
  map_df(x,function(y){
    y[map_lgl(y,is.null)] <- NA
    if(!any(map(y,length) > 1)){
      y <- unlist(y)
    }
    y
  })
}

