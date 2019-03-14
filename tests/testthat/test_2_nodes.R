context("Nodes funs")

test_that("nodes funs",{

  # MAKE SURE NEO4J IS UP: neo4j start
  con <- neo4j_api$new(url = "http://localhost:7474",
                       user = "neo4j", password = "neo4jpwd")
  clear_db(con)
  drop_all_constraints(con)

  # Test functions with no nodes
  expect_equal(NULL,get_nodes_table(con = con))

  # Create Nodes
  new_nodes <- list(list(nam = "x", id = "newnode"))
  create_nodes(new_nodes,con = con)
  expect_equal(get_node_count(con = con),1)

  # Upload nodes from CSV
  csv_url <- "https://raw.githubusercontent.com/jpmarindiaz/neo4rutils/master/inst/data/movies.csv"
  load_nodes_csv(csv_url, label = "Movie", con = con)

  expect_equal(get_node_count(con = con),5)

  create_constraint("Movie","uid", con = con)
  create_constraint("Person","uid", con = con)

  expect_equal(get_constraints(con) %>% pull(property_keys),c("uid","uid"))
  drop_constraint("Person","uid", con = con)
  expect_equal(get_constraints(con) %>% pull(property_keys),"uid")

  expect_equal(get_node_count("Movie", con = con), 4)

  expect_equal(get_node_count(con = con),5)
  expect_equal(get_edge_count(con = con),0)

  keys <- get_node_keys(con = con, asTable = TRUE)
  expect_true(all(c("year","id","title","country","nam") %in% keys$key))

  new_node <- list(id = "New movie", country = "COL", vals = "valsss")
  node <- create_nodes(list(new_node),label = "Movie", con = con)
  expect_equal(get_node_count("Movie",con),5)


  new_nodes <- list(list(id = "new-1", country = "USA", vals = c("val1", "val2")),
                    list(id = "new-2", country = "COL", values = c("x", "y")))
  nodes <- create_nodes(new_nodes,label = "Movie", con = con)
  expect_equal(get_node_count("Movie",con),7)

  expect_equal(nrow(get_nodes_table(label = "Movie", con)), 7)
  delete_node_by_uid("new-1",prop = "id","Movie",con = con)
  delete_node_by_uid("new-2",prop = "id","Movie",con = con)
  expect_equal(nrow(get_nodes_table(label = "Movie", con)), get_node_count("Movie",con))

  # Test get nodes table
  all_nodes <- get_nodes_table(con = con)
  movies <- get_nodes_table(label = "Movie", con = con)

  na_labels <- get_nodes_table(label = NA, con = con)
  #create_nodes(list(list(id = "No label", country = "RUS")),label = NULL, con = con)
  #na_labels <- get_nodes_table(label = NA, con = con)
  #delete_node_by_uid("No label",uid = "id",label = NULL,con = con)

  all_nodes2 <- bind_rows(na_labels,movies)
  expect_equal(all_nodes,all_nodes2 %>% select(-.deleted, -.type))
  expect_equal(nrow(all_nodes),nrow(all_nodes2))
  expect_error(get_nodes_table(label = "NonExistent",con),"label not in Labels")

  movies_url <- read_csv(csv_url, col_types = cols(.default = "c"))
  movies2 <- movies[names(movies_url)] %>% filter(!is.na(uid))
  expect_equal(nrow(movies_url),nrow(movies2))
  expect_equal(names(movies_url),names(movies2))


  delete_node(sample(movies$.id,1),con)
  expect_equal(get_node_count("Movie",con),4)


  delete_labeled_nodes(label = "Movie",con = con)
  expect_equal(get_label_node_count("Movie",con),0)

  people <- read_csv(system.file("data/persons.csv",package = "neo4rutils"))
  load_nodes_df(people,"Person",con)

  expect_equal(get_label_node_count("Person",con),6)

  delete_node_by_uid("p6",label = "Person", con = con)

  people <- get_nodes_table("Person",con)
  movies <- get_nodes_table(label = "Movie",con = con)
  load_nodes_csv(csv_url, "Movie", con)
  allNodes <- get_nodes_table(con = con)
  expect_true(nrow(allNodes) == 10)

  random_id <- sample(allNodes$.id,1)
  n <- get_node_by_id(random_id, con = con)
  expect_true(n$.id == random_id)

  expect_error(get_node_by_uid(uid = "p1" , prop = "uid", label = "Person", con = con),
               "id needs to be a unique constraint for label: Person")
  get_constraints(con)
  create_constraint(label = "Person", "uid", con)
  get_node_count_by_label(con)

  drop_all_constraints(con)
  expect_error(get_node_by_uid(uid = "p1", prop = "uid", label = "Person", con = con), "uid needs to be a unique constraint for label: Person")

  create_constraint("Movie","uid", con = con)
  create_constraint("Person","uid", con = con)

  n <- get_node_by_uid(uid = "p1", prop = "uid", label = "Person", con = con)
  expect_equal(n$uid, "p1")
  n <- get_node_by_uid(uid = "la-estrategia", label = "Movie", con = con)
  expect_equal(n$title, "La estrategia del caracol")
  # Get node by uid with no label
  n <- get_node_by_uid(uid = "p1", prop = "uid", con = con)

  #n <- get_node_by_prop(id = "newnode", prop = "id", con = con)
  expect_equal(n$uid, "p1")

})
