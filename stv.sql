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
  -- There might be some convenience in pulling cooked_ballots out
  -- of this query entirely and making it a view.
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
  -- We might elect more than one person in a round (if more than one
  -- person has reached quota), but we only transfer one set of
  -- ballots each time. Any untransferred ballots for elected
  -- candidates are marked "pending_xfer", but are otherwise unchanged
  -- other than by removal of retiring candidates from their remainder
  -- list. If there are pending transfers, then exclusions are not
  -- processed.
  --
  -- The "priority" field is used to break ties when transferring
  -- votes or eliminating candidates. The actual value is not
  -- important, but the ranking must reflect the counting history; the
  -- candidate with the larger vote count on the most recent round in
  -- which their vote counts differed must have the higher priority.
  -- We do this by using, after the first round, a ranking over (order
  -- by votes, priority) using the current round's votes and the
  -- previous round's priority.
  --
  -- An additional dummy row (with candidate=NULL, final_result=true)
  -- is issued for every round, with a json blob in the "annotations"
  -- column; this is to allow inspection of the intermediate counts
  -- and report the details of the count process.
  work_ballots(
	  candidate,	-- candidate at the front of the prefs list
	  remainder,	-- remainder of the prefs list, if any
	  priority,		-- elimination priority of this candidate
	  vote_value,	-- value of this ballot for this candidate
	  vacancies,	-- number of remaining vacancies
  	  round,		-- which round this is
	  final_result, -- "final" result flag
	  pending_xfer, -- "pending transfer" flag
	  annotations	-- json annotations added in processing
  ) as (select prefs[1],
			   prefs[2:],
			   count(*) over (partition by prefs[1]),
			   1::numeric,
			   (select vacancies from cdata),
			   1,
			   false,
			   false,
			   null::jsonb
		  from cooked_ballots
		 where prefs[1] is not null
		union all
		select c.candidate_id,
			   array[]::integer[],
			   0,
			   0,
			   (select vacancies from cdata),
			   1,
			   false,
			   false,
			   null::jsonb
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
			 as (select *,
			            s.votes - s.quota as surplus,
			            (s.votes - s.quota)/s.votes as transfer_value
				   from (select w.candidate,
								max(w.priority) as priority,
								max(w.round) as round,
								sum(w.vote_value) as votes,
								(select quota from cdata) as quota,
								max(w.vacancies) as vacancies,
								bool_or(pending_xfer) as pending_xfer,
								count(*) over () as num_candidates
						   from w
						  group by candidate) s
				  where s.num_candidates > s.vacancies),
		   -- first elect all the candidates over quota.
		   elect
			 as (select * from c_score where surplus >= 0 and not pending_xfer),
		   -- if nobody was elected this round, and there are no
		   -- surpluses yet to transfer, we must exclude someone. the
		   -- excluded candidate must have the lowest number of votes,
		   -- and in case of a tie, the lowest priority, and in case
		   -- of a tie of that, the lowest precast lot for this round.
		   -- Again, just return an empty result if the election is
		   -- ending by exhaustion.
		   exclude
			 as (select cs.candidate,
						cs.quota,
						cs.round
				   from c_score cs
				   join cur_candidates c on (cs.candidate = c.candidate_id)
				  where not exists (select 1 from c_score where surplus >= 0)
				  order by cs.votes, cs.priority, c.precast_lots[round]
				  limit 1),
		   -- we need to decide, if there are any surpluses or
		   -- exclusions, whose votes will be transferred this round.
		   -- We consider both candidates with pending transfers (who
		   -- are already elected) and any candidates with surpluses
		   -- this round. We then arrange to transfer the votes of the
		   -- candidate with the largest surplus, or the highest
		   -- priority, or the highest precast lot for this round.
		   -- Note that a candidate might be elected with no surplus
		   -- if they hit the quota exactly; if so, we still transfer
		   -- out a pending surplus if one exists. If we're excluding
		   -- a candidate instead, then there will be only one row
		   -- chosen according to the _lowest_ votecount/priority
		   -- already.
		   surpluses
		     as (select s.candidate,
			            s.transfer_value,
						s.ranking = 1 as do_transfer
				   from (select cs.candidate, cs.transfer_value,
				   				row_number() over (order by cs.votes desc,
						                                 	cs.priority desc,
															c.precast_lots[round] desc)
								  as ranking
				           from c_score cs
				           join cur_candidates c on (cs.candidate=c.candidate_id)
						  where cs.surplus > 0
						 union all
						 select e.candidate, 1, 1
						   from exclude e) s),
		   -- this is the candidate(s) we're retiring from the election in
		   -- this round, whether by election or elimination or exhaustion.
		   -- In the exhaustion case, this will be every remaining candidate
		   -- (who are all treated as elected).
		   retire(
			   candidate,
			   quota,
			   round,
			   nelect
		   ) as (select candidate, quota, round, 1
				   from elect
				 union all
				 select candidate, 0, round, 0
				   from exclude
				 union all
				 select w.candidate,
						(select quota from cdata),
						max(w.round),
						1
				   from w
				  where not exists (select 1 from c_score)
				  group by w.candidate),
		   -- for the candidate chosen for transfer, we generate a new
		   -- set of rows for their transferred ballots. The transfer
		   -- value is determined by the elected/excluded candidate,
		   -- but the priority must be updated (later) to match the
		   -- new first preference.
		   transfer
			 as (select s.remainder[1] as candidate,
						s.remainder[2:] as remainder,
						trunc(s.vote_value * s.transfer_value,5)
						  as vote_value,
						s.vacancies,
						s.round,
						false as pending_xfer
				   from (select w.vote_value,
				   				sp.transfer_value,
								w.vacancies,
								w.round,
 			                    array(select c_id
						                from unnest(w.remainder) with ordinality as u(c_id,ord)
						               where c_id not in (select candidate from retire)
							           order by ord)
						          as remainder
						   from w
				   		   join surpluses sp on (sp.candidate=w.candidate)
				  		  where sp.do_transfer) s
				  where s.remainder[1] is not null),
		   -- collect all existing ballots that will proceed to next
		   -- round; we must remove the retiring candidates from the
		   -- preference lists. Be sure to preserve order when
		   -- removing candidates.
		   keep
			 as (select w.candidate,
			            array(select c_id
						        from unnest(w.remainder) with ordinality as u(c_id,ord)
						       where c_id not in (select candidate from retire)
							   order by ord)
						  as remainder,
						w.vote_value,
						w.vacancies,
						w.round,
						s.candidate is not null as pending_xfer
				   from w
				   left join surpluses s on (s.candidate=w.candidate)
				  where s.do_transfer is not true),
		   -- compile annotations
		   annotation(round,json)
		     as (select r.round,
			            to_jsonb(a)
			       from (select round from w limit 1) r,
				        (select (select json_agg(c_score) from c_score) as c_score,
								(select json_agg(s) from surpluses s) as surpluses,
								(select json_agg(e) from exclude e) as exclude,
								(select json_agg(k) from (select candidate, vote_value, count(*)
								 						    from keep
														   group by candidate, vote_value) k)
								  as keep,
								(select json_agg(t) from (select candidate, vote_value, count(*)
								                            from transfer
														   group by candidate, vote_value) t)
								  as transferred_ballots,
								(select count(*) from keep) as n_keep,
								(select count(*) from transfer) as n_xfer
						) a),
		   -- prepare the new ballot list for next time, and append
		   -- the result row for the retiring candidates. Also output
		   -- an annotation row for the round, so that the caller can
		   -- document the count process.
		   new_ballots
			 as (select s.candidate,
						s.remainder,
						dense_rank() over (order by cs.votes, cs.priority)
						  as priority,
						s.vote_value,
						s.vacancies - (select coalesce(sum(nelect)::integer,0) from retire) as vacancies,
						s.round + 1 as round,
						false as final_result,
						s.pending_xfer,
						null::jsonb
				   from (select * from transfer
						  union all
						 select * from keep) s
				   join c_score cs on (s.candidate=cs.candidate)
				 union all
				 select r.candidate,
						null,
						null,
						r.quota,	-- 0 for excluded, quota for elected
						null,
						r.round,
						true,
						false,
						null
				   from retire r
				 union all
				 select null, null, null, null, null, a.round, true, false, a.json
				   from annotation a)
		   -- return output to next pass
		   select * from new_ballots)
)
-- final output:
select candidate,
	   candidate_name,
	   case when vote_value > 0 then 'ELECTED'
	        when vote_value = 0 then 'ELIMINATED'
		end as result,
	   round,
	   jsonb_pretty(annotations)
  from work_ballots w
  left join cur_candidates c on (w.candidate=c.candidate_id)
 where final_result
 order by vote_value desc nulls last, round;
