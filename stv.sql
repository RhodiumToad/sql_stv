
with recursive
  -- common data about the whole election:
  --  vacancies = how many candidates to elect
  --  num_ballots = total valid ballots cast
  --  quota = the quota for election
  cdata
    as (select vacancies,
               num_ballots,
               (num_ballots / (vacancies + 1)) + 1 as quota
          from (select 4 /*PARAMETER*/ as vacancies,
                       count(*) as num_ballots
                  from ballots) s),
  -- "work_ballots" at each stage of the recursion is the working list
  -- of ballots. Initially, all ballots have "final_result" = false;
  -- when a candidate is elected or eliminated, we issue them a dummy
  -- ballot with "final_result"=true, which becomes the query result.
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
      candidate,    -- candidate at the front of the prefs list
      remainder,    -- remainder of the prefs list, if any
          priority,     -- elimination priority of this candidate
          vote_value,   -- value of this ballot for this candidate
          vacancies,    -- number of remaining vacancies
          final_result, -- "final" result flag
          round         -- which round this is
  ) as (select prefs[1],
               prefs[2:],
               count(*) over (partition by prefs[1]),
               1::numeric,
               (select vacancies from cdata),
               false,
               1
          from ballots
         where prefs[1] is not null
         union all
                -- recursive term starts here.
        (with
               -- we can only access the recursive term once, so suck it
               -- into a CTE.
           w as (select * from work_ballots
                              where not final_result
                                and vacancies > 0),
                   -- compute current totals by candidate; also note priority
                   -- and round for later use (the min() is redundant, the
                   -- priority and round is the same between all votes for a
                   -- single candidate). Get the quota here as well for later
                   -- computations to use.
           c_score
                     as (select w.candidate,
                                min(w.priority) as priority,
                                                min(w.round) as round,
                                            sum(w.vote_value) as votes,
                                                (select quota from cdata) as quota
                               from w
                              group by candidate),
           -- first elect at most one of the candidates over quota.
           -- if more than one is, we must elect the one with the
           -- largest surplus (= most votes), or the highest priority,
           -- or the highest precast lot for this round.
           elect
                     as (select cs.candidate,
                        cs.quota,
                        (cs.votes - cs.quota)/cs.votes as transfer_value,
                        cs.round
                   from c_score cs
                   join candidates c on (cs.candidate=c.candidate_id)
                                  where cs.votes >= cs.quota
                  order by cs.votes desc, cs.priority desc, c.precast_lots[round] desc
                  limit 1),
           -- if nobody was elected this round, we must exclude
           -- someone. the excluded candidate must have the lowest
           -- number of votes, and in case of a tie, the lowest
           -- priority, and in case of a tie of that, the lowest
           -- precast lot for this round.
           exclude
                     as (select cs.candidate,
                                    cs.quota,
                                cs.round
                   from c_score cs
                   join candidates c on (cs.candidate = c.candidate_id)
                                  where not exists (select 1 from elect)
                  order by cs.votes, cs.priority, c.precast_lots[round]
                  limit 1),
           -- this is the candidate we're retiring from the election in
                   -- this round, whether by election or elimination.
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
                               from exclude),
           -- for the candidate elected or excluded, we generate a new
           -- set of rows for their transferred ballots. The transfer
           -- value is determined by the elected/excluded candidate,
           -- but the priority must be updated to match the new first
           -- preference.
           transfer
                     as (select w.remainder[1] as candidate,
                        w.remainder[2:] as remainder,
                        trunc(w.vote_value * retire.transfer_value,5)
                                                  as vote_value,
                        w.vacancies,
                        w.round
                   from w
                   join retire on (retire.candidate=w.candidate)
                                  where w.remainder[1] is not null),
           -- collect all existing ballots that will proceed to next
           -- round; we must remove the retiring candidate from the
           -- preference lists
           keep
                     as (select w.candidate,
                        array_remove(w.remainder,
                                                     (select candidate from retire))
                                              as remainder,
                        w.vote_value,
                        w.vacancies,
                        w.round
                   from w
                  where w.candidate <> (select candidate from retire)),
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
                                  where s.vote_value > 0
                                  union all
                 select candidate,
                                        null,
                                                null,
                                                quota,  -- 0 for excluded, quota for elected
                                                null,
                                                true,
                                                round
                                   from retire)
               -- return output to next pass
           select * from new_ballots))
-- final output:
select candidate,
       candidate_name,
       vote_value,
       round
  from work_ballots w
  join candidates c on (w.candidate=c.candidate_id)
 where final_result
 order by vote_value desc, round;
