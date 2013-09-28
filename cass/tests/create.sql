/* using clqsh -3 */
create keyspace test
    with strategy_class = 'SimpleStrategy' and strategy_options:replication_factor = 1;

use test;

create table users (
  key text primary key,
  email text,
  fn text,
  ln text,
  loc text,
  dj timestamp
);
create index str_user_loc ON users (loc);

create table streams (
  key uuid primary key,
  title text,
  des text,
  owner text,
  cat text
);
create index str_cat_key ON streams (cat);


/* using Cassandra-CLI */
create column family user_streams
with comparator = TimeUUIDType
and key_validation_class=UTF8Type
and default_validation_class = UTF8Type;