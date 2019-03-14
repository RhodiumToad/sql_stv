--

prepare election_results(integer) as
with recursive
  -- parameters
  params(election_id) as (values ($1)),
  -- candidates in this election
  cur_candidates
    as (select *
	      from candidates
		 where election_id = (select election_id from params)),
  --
  -- This takes the raw ballot prefs and presents them in the original
  -- format (array of candidate ids in preference order). The
  -- following considerations apply:
  --
  -- 1. ("undervoting") It's explicitly legal for a ballot to omit
  -- candidates (we allow either null preference or missing row to
  -- stand for this). The list of preferences reflects only the
  -- candidates marked, and the ballot will be discarded if it gets to
  -- the end of its preference list. A ballot with no valid
  -- preferences at all must be discarded (since it must not affect
  -- the quota).
  --
  -- 2. ("overvoting") If a preference value appears more than once,
  -- then this is considered an overvote and invalidates the
  -- preference and all numerically higher preference values, but if
  -- any lower (more preferred) preferences exist they are still
  -- honoured. Again, if the first preference is invalid, then we have
  -- to discard the ballot outright.
  --
  -- 3. Otherwise, only the relative numeric ranking of preferences
  -- matters, the actual values do not.
  --
  -- Implementation: we detect duplicate prefs by using a count(*)
  -- over the peers of each row; if it's not 1 then it's invalid,
  -- and we use an every() (aka bool_and()) aggregate to propagate
  -- the invalidity to all lower (less-preferred) preferences.
  --
  cooked_ballots
    as (select election_id,
	   		   voter_id,
	   		   array_agg(candidate_id order by preference)
	     	     filter (where valid_pref)
		 		 as prefs
  		  from (select election_id,
               		   voter_id,
			   		   candidate_id,
			   		   preference,
			   		   every(valid_pref)
			     	     over (partition by election_id, voter_id
				           	  	   order by preference asc
					       	   	   range between unbounded preceding
						                     and current row)
				 		 as valid_pref
		  		  from (select election_id,
		                	   voter_id,
					   		   candidate_id,
					   		   preference,
					   		   1 = count(*) over (partition by election_id, voter_id
				           				 	  	 	  order by preference asc
					                          		  range current row)
					             as valid_pref
				  		  from ballot_preferences
				  		 where election_id = (select election_id from params)
						   and preference is not null) s1
	           ) s2
		 group by election_id, voter_id
        having bool_or(valid_pref)),
  -- common data about the whole election:
  --  vacancies = how many candidates to elect
  --  num_ballots = total valid ballots cast
  --  quota = the quota for election
  -- the use of integer division for the quota calculation is intentional
  cdata
	as (select vacancies,
			   num_ballots,
			   (num_ballots / (vacancies + 1)) + 1 as quota
		  from (select (select count(*) from cooked_ballots) as num_ballots,
		               e.num_vacancies as vacancies
				  from elections e
				 where e.election_id = (select election_id from params)
				offset 0) s),
  -- "work_ballots" at each stage of the recursion is the working list
  -- of ballots. Initially, all ballots have "final_result" = false;
  -- when a candidate is elected or eliminated, we issue them a dummy
  -- ballot with "final_result"=true, which becomes the query result.
  --
  -- Another kind of dummy ballot, with vote_value=0, is issued at the
  -- outset to every candidate to guarantee that all candidates are
  -- represented (until elected or eliminated) in the ballot list.
  -- This simplifies the handling of candidates with no
  -- first-preference votes at some counting stage (such candidates
  -- might be eliminated, but might not). A subtlety is that this
  -- dummy ballot has priority=0, which won't match the priority of
  -- the candidate's real ballots (if any); but we are careful to use
  -- max() when computing the effective priority, so the 0 only shows
  -- up if there are no other ballots, in which case 0 is the correct
  -- priority to use.
  --
  -- The "priority" field is used to break ties when eliminating or
  -- electing candidates. The actual value is not important, but the
  -- ranking must reflect the counting history; the candidate with the
  -- larger vote count on the most recent round in which their vote
  -- counts differed must have the higher priority. We do this by
  -- using, after the first round, a ranking over (order by votes,
  -- priority) using the current round's votes and the previous
  -- round's priority.
  work_ballots(
	  candidate,	-- candidate at the front of the prefs list
	  remainder,	-- remainder of the prefs list, if any
	  priority,		-- elimination priority of this candidate
	  vote_value,	-- value of this ballot for this candidate
	  vacancies,	-- number of remaining vacancies
	  final_result, -- "final" result flag
	  round			-- which round this is
  ) as (select prefs[1],
			   prefs[2:],
			   count(*) over (partition by prefs[1]),
			   1::numeric,
			   (select vacancies from cdata),
			   false,
			   1
		  from cooked_ballots
		 where prefs[1] is not null
		union all
		select c.candidate_id,
			   array[]::integer[],
			   0,
			   0,
			   (select vacancies from cdata),
			   false,
			   1
		  from cur_candidates c
		union all
		-- recursive term starts here.
		(with
		   -- we can only access the recursive term once, so suck it
		   -- into a CTE.
		   w as (select * from work_ballots
				  where not final_result
					and vacancies > 0),
		   -- compute current totals by candidate; also note priority
		   -- and round for later use. Get the quota here as well for
		   -- later computations to use.
		   --
		   -- However, if at this step the number of remaining
		   -- candidates does not exceed the number of remaining
		   -- vacancies, the election is over and everyone remaining
		   -- is to be elected. The simplest way to deal with this is
		   -- to leave c_score empty (since we have no further use for
		   -- it), which will force various intermediate tables below
		   -- to also be empty.
		   c_score
			 as (select *
				   from (select w.candidate,
								max(w.priority) as priority,
								max(w.round) as round,
								sum(w.vote_value) as votes,
								(select quota from cdata) as quota,
								max(w.vacancies) as vacancies,
								count(*) over () as num_candidates
						   from w
						  group by candidate) s
				  where s.num_candidates > s.vacancies),
		   -- first elect at most one of the candidates over quota. if
		   -- more than one is, we must elect the one with the largest
		   -- surplus (= most votes), or the highest priority, or the
		   -- highest precast lot for this round. The exhaustion of
		   -- candidates is not handled here and we just return an
		   -- empty result for that case.
		   elect
			 as (select cs.candidate,
						cs.quota,
						(cs.votes - cs.quota)/cs.votes as transfer_value,
						cs.round
				   from c_score cs
				   join cur_candidates c on (cs.candidate=c.candidate_id)
				  where cs.votes >= cs.quota
				  order by cs.votes desc, cs.priority desc, c.precast_lots[round] desc
				  limit 1),
		   -- if nobody was elected this round, we must exclude
		   -- someone. the excluded candidate must have the lowest
		   -- number of votes, and in case of a tie, the lowest
		   -- priority, and in case of a tie of that, the lowest
		   -- precast lot for this round. Again, just return an empty
		   -- result if the election is ending by exhaustion.
		   exclude
			 as (select cs.candidate,
						cs.quota,
						cs.round
				   from c_score cs
				   join cur_candidates c on (cs.candidate = c.candidate_id)
				  where not exists (select 1 from elect)
				  order by cs.votes, cs.priority, c.precast_lots[round]
				  limit 1),
		   -- this is the candidate(s) we're retiring from the election in
		   -- this round, whether by election or elimination or exhaustion.
		   -- In the exhaustion case, this will be every remaining candidate
		   -- (who are all treated as elected).
		   retire(
			   candidate,
			   transfer_value,
			   quota,
			   round,
			   nelect
		   ) as (select candidate, transfer_value, quota, round, 1
				   from elect
				 union all
				 select candidate, 1.0, 0, round, 0
				   from exclude
				 union all
				 select w.candidate,
						0,
						(select quota from cdata),
						max(w.round),
						(count(*) over ())::integer
				   from w
				  where not exists (select 1 from c_score)
				  group by w.candidate),
		   -- for the candidate elected or excluded, we generate a new
		   -- set of rows for their transferred ballots. The transfer
		   -- value is determined by the elected/excluded candidate,
		   -- but the priority must be updated (later) to match the
		   -- new first preference.
		   transfer
			 as (select w.remainder[1] as candidate,
						w.remainder[2:] as remainder,
						trunc(w.vote_value * retire.transfer_value,5)
						  as vote_value,
						w.vacancies,
						w.round
				   from w
				   join retire on (retire.candidate=w.candidate)
				  where w.remainder[1] is not null
					and retire.transfer_value > 0),
		   -- collect all existing ballots that will proceed to next
		   -- round; we must remove the retiring candidate from the
		   -- preference lists. (If there's more than one retiring
		   -- candidate, then the election is over, all candidates are
		   -- retiring, and so we never reach the scalar subselect
		   -- that would choke on multiple rows.)
		   keep
			 as (select w.candidate,
						array_remove(w.remainder,
									 (select candidate from retire))
						  as remainder,
						w.vote_value,
						w.vacancies,
						w.round
				   from w
				  where w.candidate not in (select candidate from retire)),
		   -- prepare the new ballot list for next time, and append the
		   -- result row for the retiring candidate
		   new_ballots
			 as (select s.candidate,
						s.remainder,
						dense_rank() over (order by cs.votes, cs.priority)
						  as priority,
						s.vote_value,
						s.vacancies - (select nelect from retire) as vacancies,
						false as final_result,
						s.round + 1 as round
				   from (select * from transfer
						  union all
						 select * from keep) s
				   join c_score cs on (s.candidate=cs.candidate)
				 union all
				 select candidate,
						null,
						null,
						quota,	-- 0 for excluded, quota for elected
						null,
						true,
						round
				   from retire)
		   -- return output to next pass
		   select * from new_ballots)
)
-- final output:
select candidate,
	   candidate_name,
	   vote_value,
	   round
  from work_ballots w
  join cur_candidates c on (w.candidate=c.candidate_id)
 where final_result
 order by vote_value desc, round;
