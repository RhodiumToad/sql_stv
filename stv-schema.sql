

create table candidates
(
    candidate_id integer not null,
    candidate_name text,
    precast_lots float8[]
);

create table ballots
(
    ballot_id integer not null generated always as identity,
    prefs integer[]
);

