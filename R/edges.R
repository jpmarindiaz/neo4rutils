
create_edge <- function(src_id, tgt_id, rel_type, props, show_query = FALSE){
  src_id <- 204
  tgt_id <- 205
  rel_type <- "RELATION"
  props <- list(x = 23, txt = "weeeee")
  #ed <- list(from = 1, to = "la-estrategia", myreltype = "RELATED")
  props <- write_props_cypher(props)
  qtpl <- "MATCH (n1), (n2)
  WHERE ID(n1) = {src_id} AND ID(n2) = {tgt_id}
  CREATE (n1)-[r:{rel_type}{{props}}]->(n2)
  RETURN n1,r,n2"
  vals <- list(src_id = src_id, tgt_id = tgt_id, rel_type = rel_type, props = props)
  q <- str_tpl_format(qtpl,vals)
  if(show_query) message(q)
  call_api(q, con)
}


load_edges_csv <- function(csv_url,
                           src_col = src_col,
                           src_label = src_label,
                           src_uid_prop = src_uid_prop,
                           tgt_col = tgt_col,
                           tgt_label = tgt_label,
                           tgt_uid_prop = tgt_uid_prop,
                           rel_type = NULL,
                           con = con,
                           show_query = TRUE){

  #csv_url <- "https://raw.githubusercontent.com/jpmarindiaz/RNeo4jUtils/master/inst/data/roles.csv"
  d <- read_csv(csv_url, n_max = 5)

  # src_col <- "personId"
  # tgt_col <- "movieId"
  # src_uid_prop <- "id"
  # tgt_uid_prop <- "id"
  # src_label <- "Person"
  # tgt_label <- "Movie"
  # rel_type <- "ROLESSSSSS"
  # rel_props_cols <- c("role","personId")

  props <- paste0("csvLine.",rel_props_cols)
  names(props) <- rel_props_cols
  props <- write_props_cypher(as.list(props), quote = FALSE)
  qtpl <- '
  MERGE (n1:{src_label}{{src_uid_prop}:csvLine.{src_col}})
  MERGE (n2:{tgt_label}{{tgt_uid_prop}:csvLine.{tgt_col}})
  MERGE(n1)-[r:TESTtttt{{props}}]-(n2)
  RETURN n1,r,n2'
  vals <- list(src_id = src_id, src_label = src_label,
               src_col = src_col, src_uid_prop = src_uid_prop,
               tgt_id = tgt_id, tgt_label = tgt_label,
               tgt_col = tgt_col, tgt_uid_prop = tgt_uid_prop,
               rel_type = rel_type, rel_type_col = rel_type_col,
               props = props)
  on_load_query <- str_tpl_format(qtpl, vals)
  if(show_query) message(on_load_query)
  load_csv(url = csv_url,
           con = con, header = TRUE, periodic_commit = 50,
           as = "csvLine", on_load = on_load_query)

  # USING PERIODIC COMMIT 50 LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/jpmarindiaz/RNeo4jUtils/master/inst/data/roles.csv' AS csvLine
  # MERGE (n1:Person{id:csvLine.personId})
  # MERGE (n2:Movie{id:csvLine.movieId})
  # MERGE (n1)-[r:TEST2333]->(n2)

}




#' @export
get_edges_table <- function(rel_type = NULL, con = NULL){
  if(is.null(rel_type)){
    rel_types <- con$get_relationships() %>% pull(1)
    edges <- map(rel_types, ~get_edges_rel_type_table(.,con)) %>%
      bind_rows()
  }else{
    edges <- get_edges_rel_type_table(rel_type, con)
  }
  edges
}


get_edges_rel_type_table <- function(rel_type, con){
  q <- glue("MATCH (n1)-[r:{rel_type}]->(n2) RETURN n1,r,n2")
  glue(q)
  res <- call_api(q, con, meta = TRUE)
  edges <- res$r %>% select(rel_id = id, everything(), -type, -deleted)
  n1 <- res$n1 %>% select(src_id = id)
  n2 <- res$n2 %>% select(tgt_id = id)
  edges <- bind_cols(n1,edges,n2) %>%
    mutate(rel_type = rel_type) %>%
    select(rel_type, rel_id, src_id, tgt_id, everything())
}




#' @export
get_total_edge_count <- function(graph){
  q <- "MATCH (n)-[r]-(m)
    RETURN COUNT(r)"
  flatten_int(cypher(graph, q))
}



#' @export
load_edges_data_frame <- function(edges,
                                  sourceCol = NULL, targetCol = NULL,
                                  relType = NULL, relTypeCol = NULL,
                                  sourceProp = NULL, targetProp = NULL,
                                  sourceLabel = NULL, targetLabel = NULL,
                                  graph = NULL,createNodes = FALSE){
  ed <- transpose(edges)
  f <- function(e){
    #e <- ed[[1]]
    props <- e
    props[sourceCol] <- NULL
    props[targetCol] <- NULL
    src <- get_node_by_uid(e[[sourceCol]],prop = sourceProp, label = sourceLabel,
                           graph = graph)
    tgt <- get_node_by_uid(e[[targetCol]],prop = targetProp, label = targetLabel,
                           graph = graph)
    if(is.null(src) || is.null(tgt))
      return(paste0("ERROR in src:",e[[sourceCol]], ", tgt: ",e[[targetCol]]))
    if(!is.null(relTypeCol)){
      if(!relTypeCol %in% names(e)) stop("RelTypeCol not in node")
      relType <- e[[relTypeCol]]
    }
    props <- props[!is.na(props)]
    if(is.na(relType))
      return(paste0("ERROR: REL_TYPE is NA in src:",e[[sourceCol]], ", tgt: ",e[[targetCol]]))

    if(length(props) == 0)
      props <- NULL

    createRel(src, relType, tgt, props)
    "OK"
  }
  map(ed,f)
}

#' @export
load_edges_csv <- function(file, sourceCol = NULL, targetCol = NULL,
                           relType = NULL,
                           sourceProp = NULL, targetProp = NULL,
                           sourceLabel = NULL, targetLabel = NULL,
                           graph = NULL,createNodes = FALSE){
  edges <- read_csv(file)
  load_edges_data_frame(edges,sourceCol = sourceCol, targetCol = targetCol,
                        relType = relType,
                        sourceProp = sourceProp, targetProp = targetProp,
                        sourceLabel = sourceLabel, targetLabel = targetLabel,
                        graph = graph,createNodes = FALSE)
}


#' @export
get_edge_count <- function(relType= NULL, con = con){
  if(!is.null(relType)){
    q <- "MATCH (n)-[r:`{relType}`]-(m)
    RETURN COUNT(r)"
    q <- str_tpl_format(q,list(relType = relType))
  }else{
    q <- "MATCH (n)-[r]-(m)
    RETURN COUNT(r)"
  }
  unname(unlist(call_api(q, con)))
}

