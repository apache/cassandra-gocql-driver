CREATE KEYSPACE gocqlx_aggregates WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '2'
};

CREATE FUNCTION gocqlx_aggregates.avgstate(
  state tuple<int, double>,
  val double)
CALLED ON NULL INPUT
RETURNS frozen<tuple<int, double>>
LANGUAGE lua
AS $$
  return { state[1]+1, state[2]+val }
  $$;

CREATE FUNCTION gocqlx_aggregates.avgfinal(
  state tuple<int, double>)
CALLED ON NULL INPUT
RETURNS double
LANGUAGE lua
as $$
  r=0
  r=state[2]
  r=r/state[1]
  return r
  $$;

CREATE AGGREGATE gocqlx_aggregates.average(double)
SFUNC avgstate STYPE tuple<int, double>
FINALFUNC avgfinal
INITCOND (0,0.0);
