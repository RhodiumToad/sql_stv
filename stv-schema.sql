--

create table elections
(
	election_id integer not null primary key,
	election_name text,
	num_vacancies integer not null check(num_vacancies > 0)
);

-- There's no particular need for the lots to be floats, they can be
-- anything sortable. Sticklers for good randomness should probably
-- use gen_random_bytes instead, changing the column type to bytea[].

create table candidates
(
	election_id integer not null references elections,
    candidate_id integer not null,
    candidate_name text,
    precast_lots float8[],

	primary key (election_id, candidate_id)
);

--
-- Unlike the original version, this ballot table has multiple rows
-- per physical ballot cast: one row per candidate per ballot.
--
-- Think not to try and enforce preference uniqueness with a unique
-- constraint here, because a ballot with overvotes must still be
-- counted up to the point of the overvote.
--
create table ballot_preferences
(
	election_id integer not null,
	voter_id integer not null,
	candidate_id integer not null,
	preference integer,

	primary key (election_id, voter_id, candidate_id),
	foreign key (election_id, candidate_id) references candidates
);
