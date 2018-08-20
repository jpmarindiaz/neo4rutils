context("Edges funs")

test_that("edges funs",{

  # MAKE SURE NEO4J IS UP: neo4j start

  #dotenv::load_dot_env(".env")
  #neo4j <- as.list(Sys.getenv(c("NEO4J_URL","NEO4J_USR","NEO4J_PWD")))
  # con <- neo4j_api$new(url = neo4j$NEO4J_URL,
  #                      user = neo4j$NEO4J_USR, password = neo4j$NEO4J_PWD)

  con <- neo4j_api$new(url = "http://localhost:7474",
                       user = "neo4j", password = "neo4jpwd")
  clear_db(con)
  expect_true(con$ping() == 200)
  drop_all_constraints(con)

  csv_url <- "https://raw.githubusercontent.com/jpmarindiaz/neo4rutils/master/inst/data/movies.csv"
  load_nodes_csv(csv_url, label = "Movie", con = con)
  csv_url <- "https://raw.githubusercontent.com/jpmarindiaz/neo4rutils/master/inst/data/persons.csv"
  load_nodes_csv(csv_url, label = "Person", con = con)


  # TEST LOAD EDGES
  csv_url <- system.file("data/roles.csv",package = "neo4rutils")
  d <- read_csv(csv_url)
  nodes <- get_nodes_table(con = con)

  src_col <- "personId"
  tgt_col <- "movieId"
  rel_type <- "MYREL"
  src_uid_prop <- "uid"
  tgt_uid_prop <- "uid"
  src_label <- NULL
  tgt_label <- NULL
  rel_props <- NULL

  prep_edges_load_query(d = d, rel_type = rel_type, src_col = src_col,
                        src_label = src_label, src_uid_prop = src_uid_prop,
                        tgt_col = tgt_col, tgt_label = tgt_label,
                        tgt_uid_prop = tgt_uid_prop, rel_props = rel_props,
                        con = con, show_query = show_query)

  csv_url <- "https://raw.githubusercontent.com/jpmarindiaz/neo4rutils/master/inst/data/roles.csv"
  load_edges_csv(csv_url = csv_url,
                 rel_type = rel_type,
                 src_col = src_col,
                 src_label = src_label,
                 src_uid_prop = src_uid_prop,
                 tgt_col = tgt_col,
                 tgt_label = tgt_label,
                 tgt_uid_prop = tgt_uid_prop,
                 rel_props = rel_props,
                 con = con,
                 show_query = TRUE)
  alledges <- get_edges_table(rel_type = NULL, con)
  expect_equal(get_edge_count(rel_type = NULL,con),7)
  delete_edges(con)
  expect_equal(get_edge_count(rel_type = NULL,con),0)

  src_col <- "personId"
  tgt_col <- "movieId"
  src_uid_prop <- "uid"
  tgt_uid_prop <- "uid"
  src_label <- "Person"
  tgt_label <- "Movie"
  rel_type <- "TEST1"
  rel_props <- c("prop2","prop1")
  prep_edges_load_query(d = d, rel_type = rel_type, src_col = src_col,
                        src_label = src_label, src_uid_prop = src_uid_prop,
                        tgt_col = tgt_col, tgt_label = tgt_label,
                        tgt_uid_prop = tgt_uid_prop, rel_props = rel_props,
                        con = con, show_query = show_query)

  csv_url <- "https://raw.githubusercontent.com/jpmarindiaz/neo4rutils/master/inst/data/roles.csv"
  load_edges_csv(csv_url = csv_url,
                 rel_type = rel_type,
                 src_col = src_col,
                 src_label = src_label,
                 src_uid_prop = src_uid_prop,
                 tgt_col = tgt_col,
                 tgt_label = tgt_label,
                 tgt_uid_prop = tgt_uid_prop,
                 rel_props = rel_props,
                 con = con,
                 show_query = TRUE)

  nodes <- get_nodes_table(con = con)

  alledges <- get_edges_table(rel_type = NULL, con)
  edges1 <- get_edges_rel_type_table("TEST1", con, src_cols = c("uid","country"), tgt_cols = "uid")

  expect_equal(nrow(d), nrow(edges1))

  load_edges_csv(csv_url = csv_url,
                 rel_type = "TEST2",
                 src_col = src_col,
                 src_label = src_label,
                 src_uid_prop = src_uid_prop,
                 tgt_col = tgt_col,
                 tgt_label = NULL,
                 tgt_uid_prop = tgt_uid_prop,
                 rel_props = NULL,
                 con = con,
                 show_query = TRUE)
  edges2 <- get_edges_rel_type_table("TEST2", con)
  edges <- get_edges_table(con = con)
  expect_equal(nrow(edges), nrow(edges1) + nrow(edges2))

  ## TODO... Add edges with no REL_TYPE
  get_edge_count_by_rel_type(con)


  # ### TODO FIX rel_type
  # tmp <-create_edges(edges,
  #                    rel_type = rel_type,
  #                    src_col = src_col,
  #                    src_label = src_label,
  #                    src_uid_prop = src_uid_prop,
  #                    tgt_col = tgt_col,
  #                    tgt_label = tgt_label,
  #                    tgt_uid_prop = tgt_uid_prop,
  #                    rel_type_col = rel_type_col,
  #                    con = con,
  #                    show_query = TRUE)
  #
  # cat(tmp)



  # load_edges_data_frame(edges,
  #                       sourceCol = sourceCol, targetCol = targetCol,
  #                       relType = relType, relTypeCol = NULL,
  #                       sourceProp = sourceProp, targetProp = targetProp,
  #                       sourceLabel = sourceLabel, targetLabel = targetLabel,
  #                       con,createNodes = FALSE)
  # expect_equal(get_total_edge_count(con),14)
  #
  # load_edges_data_frame(edges,
  #                       sourceCol = sourceCol, targetCol = targetCol,
  #                       relTypeCol = relTypeCol,
  #                       sourceProp = sourceProp, targetProp = targetProp,
  #                       sourceLabel = sourceLabel, targetLabel = targetLabel,
  #                       con = con, createNodes = FALSE)
  #
  # edgesTable <- get_edges_table(con)
  #
  # edgesNew <- edges
  # edgesNew$personId[1] <- 99
  # expect_warning(load_edges_data_frame(edgesNew,
  #                       sourceCol = sourceCol, targetCol = targetCol,
  #                       relType = relType, relTypeCol = NULL,
  #                       sourceProp = sourceProp, targetProp = targetProp,
  #                       sourceLabel = sourceLabel, targetLabel = targetLabel,
  #                       con,createNodes = FALSE))
  #
  # edgesNew$personId[1] <- 1
  # edgesNew$role[1] <- NA
  # load_edges_data_frame(edgesNew,
  #                       sourceCol = sourceCol, targetCol = targetCol,
  #                       relType = NULL, relTypeCol = "role",
  #                       sourceProp = sourceProp, targetProp = targetProp,
  #                       sourceLabel = sourceLabel, targetLabel = targetLabel,
  #                       con,createNodes = FALSE)
  #
  # nMovies <- get_node_count("Movie",con)
  # expect_error(delete_node_by_uid("1","id","Movie", con = con))
  # delete_node_by_uid("1","id","Movie", con = con, withRels = TRUE)
  # expect_equal(nMovies -1, get_node_count("Movie", con = con))
  # expect_equal(get_edge_count("ROLE",con),16)
  # expect_equal(get_edge_count("ROLE",con),16)
  # expect_equal(get_total_node_count(con),12)
  #
  # allNodes <- get_nodes_table(con)
  # string <- "merixxxxxxx"
  # props <- c("id","name","country")
  # expect_true(is.null(search_nodes(string, props, caseSensitive = FALSE, con)))
  # string <- "een"
  # props <- c("id","name","country")
  # expect_true(nrow(search_nodes(string, props, caseSensitive = FALSE, con)) == 2)

})
