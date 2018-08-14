

clear_db <- function(con){
  q <- "MATCH (n)
  OPTIONAL MATCH (n)-[r]-()
  DELETE n,r;"
  call_api(q, con)
  get_node_count(con = con) == 0
}

drop_constraint <- function(label, property, con){
  q <- "DROP CONSTRAINT ON (a:{label}) ASSERT a.{property} IS UNIQUE;"
  q <- str_tpl_format(q,list(label = label, property = property))
  call_api(q, con)
  !property %in% con$get_constraints()$property_keys
}

create_constraint <- function(label, property, con){
  q <- "CREATE CONSTRAINT ON (a:{label}) ASSERT a.{property} IS UNIQUE;"
  q <- str_tpl_format(q,list(label = label, property = property))
  call_api(q, con)
  property %in% con$get_constraints()$property_keys
}

get_constraints <- function(con){
  con$get_constraints()
}
