create table champions_league_games (
  game_id BIGINT,
  host_team TEXT,
  away_team TEXT,
  game_start_time TIMESTAMPTZ,
  location TEXT,
  score TEXT,
  last_updated_at TIMESTAMPTZ
);

create table champions_league_games_staging (
  game_id BIGINT,
  host_team TEXT,
  away_team TEXT,
  game_start_time TIMESTAMPTZ,
  location TEXT,
  score TEXT,
  last_updated_at TIMESTAMPTZ,
  batch_id TEXT
);