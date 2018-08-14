context("test")

test_that("test",{

  # MAKE SURE NEO4J IS UP: neo4j start
  # dburl <- "http://localhost:7474/db/data/"
  # username <- "neo4j"
  # password <- "neo4jpwd"


  library(neo4r)

  #dotenv::load_dot_env(".env")
  #neo4j <- as.list(Sys.getenv(c("NEO4J_URL","NEO4J_USR","NEO4J_PWD")))
  # con <- neo4j_api$new(url = neo4j$NEO4J_URL,
  #                      user = neo4j$NEO4J_USR, password = neo4j$NEO4J_PWD)

  con <- neo4j_api$new(url = "http://localhost:7474",
                       user = "neo4j", password = "neo4jpwd")
  clear_db(con)
  expect_true(con$ping() == 200)

  new_nodes <- list(list(nam = "x", id = "newnode"))
  #new_nodes <- list(nam = "x", idx = 3)
  create_nodes(new_nodes,con = con)

  expect_equal(get_node_count(con = con),1)

  csv_url <- "https://raw.githubusercontent.com/jpmarindiaz/RNeo4jUtils/master/inst/data/movies.csv"
  load_nodes_csv(csv_url, label = "Movie", con = con)

  expect_equal(get_node_count(con = con),5)


  create_constraint("Movie","id", con = con)
  create_constraint("Movie","uid", con = con)
  expect_equal(con$get_constraints() %>% pull(property_keys),c("uid","id"))
  drop_constraint("Movie","id", con = con)
  expect_equal(con$get_constraints() %>% pull(property_keys),"uid")

  expect_equal(get_node_count("Movie", con = con), 4)
  get_node_count_by_label(con = con)

  # TODO get node count with out label

  expect_equal(get_node_count(con = con),5)
  expect_equal(get_edge_count(con = con),0)

  keys <- get_node_keys(con = con, asTable = TRUE)
  expect_true(all(c("year","id","title","country","nam") %in% keys$key))

  all_nodes <- get_nodes_table(con = con)
  movies <- get_nodes_table(label = "Movie", con = con)

  movies_url <- read_csv(csv_url, col_types = cols(.default = "c"))
  movies2 <- movies[names(movies_url)]
  expect_equal(movies_url,movies2)

  delete_node(sample(movies$.id,1),con)
  expect_equal(get_node_count("Movie",con),3)

  #new_node <- list(id = "New movie", country = "COL", vals = c("val1", "val2"))
  new_node <- list(id = "New movie", country = "COL", vals = "valsss")
  create_nodes(list(new_node),label = "Movie", con = con)
  expect_equal(get_node_count("Movie",con),4)


  movies <- get_nodes_table(label = "Movie", con = con)
  expect_true(!is.null(movies$vals))
  #expect_true(length(strsplit(movies$vals[4], split="||", fixed = TRUE)[[1]]) == 2)
  delete_labeled_nodes(label = "Movie",con = con)
  expect_equal(get_label_node_count("Movie",con),0)

  people <- read_csv(system.file("data/persons.csv",package = "neo4rutils"))
  load_nodes_df(people,"Person",con)
  get_node_count("Person",con)


  expect_error(get_nodes_table(label = "NonExistent",con),"label not in Labels")

  people <- get_nodes_table("Person",con)
  movies <- get_nodes_table(label = "Movie",con = con)
  expect_true(nrow(movies) == 0)
  load_nodes_csv(csv_url, "Movie", con)
  allNodes <- get_nodes_table(con = con)
  expect_true(nrow(allNodes) == 11)

  random_id <- sample(allNodes$.id,1)
  n <- get_node_by_id(random_id, con = con)
  expect_true(n$.id == random_id)

  expect_error(get_node_by_uid(uid = "p1" , prop = "id", label = "Person", con = con))
  get_constraints(con)
  create_constraint(label = "Person", "id", con)
  n <- get_node_by_uid(uid = "p1", prop = "id", label = "Person", con = con)
  expect_equal(n$id, "p1")
  n <- get_node_by_uid(uid = "la-estrategia", prop = "id", label = "Movie", con = con)
  expect_equal(n$title, "La estrategia del caracol")
  # Get node by uid with no label
  n <- get_node_by_uid(uid = "1", prop = "id", con = con)
  expect_true(n$id, "1")

  edges <- read_csv(system.file("data/roles.csv",package = "neo4rutils"))
  edges <- edges %>% select(roleX = role, everything())
  edges <- as.list(edges) %>% transpose()

  src_col <- "personId"
  tgt_col <- "movieId"
  src_uid_prop <- "id"
  tgt_uid_prop <- "id"
  src_label <- "Person"
  tgt_label <- "Movie"
  rel_type <- "ROLESSSSSS"
  rel_type_col <- "role"




  tmp <-create_edges(edges,
                     rel_type = NULL,
                     src_col = src_col,
                     src_label = src_label,
                     src_uid_prop = src_uid_prop,
                     tgt_col = tgt_col,
                     tgt_label = tgt_label,
                     tgt_uid_prop = tgt_uid_prop,
                     rel_type_col = rel_type_col,
                     con = con,
                     show_query = TRUE)

  cat(tmp)


  ### TODO FIX rel_type
  tmp <-create_edges(edges,
                     rel_type = rel_type,
                     src_col = src_col,
                     src_label = src_label,
                     src_uid_prop = src_uid_prop,
                     tgt_col = tgt_col,
                     tgt_label = tgt_label,
                     tgt_uid_prop = tgt_uid_prop,
                     rel_type_col = rel_type_col,
                     con = con,
                     show_query = TRUE)

  cat(tmp)



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
