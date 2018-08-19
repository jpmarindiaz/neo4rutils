
#' @export
clear_db <- function(con){
  q <- "MATCH (n)
  OPTIONAL MATCH (n)-[r]-()
  DELETE n,r;"
  call_api(q, con)
  get_node_count(con = con) == 0
}

#' @export
drop_constraint <- function(label, property, con){
  q <- "DROP CONSTRAINT ON (a:{label}) ASSERT a.{property} IS UNIQUE;"
  q <- str_tpl_format(q,list(label = label, property = property))
  call_api(q, con)
  !property %in% con$get_constraints()$property_keys
}


#' @export
drop_all_constraints <- function(con){
  constraints <- con$get_constraints()
  if(nrow(constraints) == 0) return()
  constraints <- constraints%>%
    select(label, property_keys) %>%
    transpose()
  all(map(constraints, ~drop_constraint(.$label, .$property_keys, con)))
}


#' @export
create_constraint <- function(label, property, con){
  q <- "CREATE CONSTRAINT ON (a:{label}) ASSERT a.{property} IS UNIQUE;"
  q <- str_tpl_format(q,list(label = label, property = property))
  call_api(q, con)
  property %in% con$get_constraints()$property_keys
}

#' @export
get_constraints <- function(con){
  con$get_constraints()
}
