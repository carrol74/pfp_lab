# Report

## Parallel Refine Rows

1. To speed up the costly refine function , we attempted a simple parallelization using `spawn_link` :

   ```erlang
   refine_rows_parallel(M) ->
       Parent = self(),
       WithIndex = lists:zip(lists:seq(1, length(M)), M),
       [spawn_link(fun() -> Parent ! {Index, refine_row(Row)} end) || {Index, Row} <- WithIndex],
       Results = [receive {I, R} -> {I, R} end || _ <- M],
       Sorted = lists:keysort(1, Results),
       [R || {_, R} <- Sorted].
   ```

2. When running `sudoku:benchmarks().`, we observed an unexpected crash:

   ![image-20250422152656995](./report_img/image-20250422152656995.png)

   Check which part of code has triggered exit and we found that when refining rows in **parallel**, the check `length(lists:usort(NewEntries)) == length(NewEntries)` in `refine_row/1` can trigger `false` 

   ![image-20250420113408690](./report_img/image-20250420113408690.png)

   By analyzing the solver's control flow, we recognized that unhandled `no_solution` exceptions during refinement terminates the solver, resulting in an  incomplete solution.

   ```erlang
   solve_one([M|Ms]) ->
        case catch solve_refined(M) of
    	{'EXIT',no_solution} ->
    	    solve_one(Ms);
    	Solution ->
    	    Solution
        end.
   ```

   We need to handle the potential errors properly and ensure that process failures are communicated back to the parent process. After modification :

   ```erlang
   refine_rows(M) ->
       Parent = self(), 
       Ref = make_ref(),
       WithIndex = lists:zip(lists:seq(1, length(M)), M),
   
       [spawn_link(fun() -> 
           Result = (catch refine_row(Row)),
           case Result of
               {'EXIT', no_solution} ->
                   Parent ! {Ref, Index, no_solution};
               ValidRow ->
                   Parent ! {Ref, Index, {ok, ValidRow}}
           end
       end) || {Index, Row} <- WithIndex],
   
       Results = [receive 
                      {Ref, _, no_solution} -> 
                          exit(no_solution);  % Propagate the error up
                      {Ref, I, {ok, Row}} -> 
                          {I, Row}
                  end || _ <- M],
   
       Sorted = lists:keysort(1, Results),
       [R || {_, R} <- Sorted].
   ```

   The benchmark results presented a consistent slowdown across all tested puzzles after implementing parallel processing in the solver. We assume that the slowdown happened because the method of splitting the work into parallel tasks ended up creating more extra work than it saved.

   | Original                                                     | Parallel                                                     |
   | ------------------------------------------------------------ | ------------------------------------------------------------ |
   | ![image-20250422161704422](./report_img/image-20250422161704422.png) | ![image-20250422161607079](./report_img/image-20250422161607079.png) |

   TODO: Performance analysis using tools?

## Parallel Solve

1. To address this, we try to parallelize at the guess level, where each process handles a fully independent puzzle state derived from a distinct guess. 

   ```erlang
   solve_one([]) ->
       exit(no_solution);
   
   solve_one(Ms) ->
       Parent = self(),
       Ref = make_ref(),
       [spawn_link(fun() ->
           Result = (catch solve_refined(M)),
           Parent ! {Ref, Result}
       end) || M <- Ms],
   
       receive
           {Ref, {'EXIT', no_solution}} ->
               solve_one_collect(Ref, length(Ms) - 1);
           {Ref, Solution} ->
               Solution
       end.
   
   solve_one_collect(_, 0) ->
       exit(no_solution);
   solve_one_collect(Ref, N) ->
       receive
           {Ref, {'EXIT', no_solution}} ->
               solve_one_collect(Ref, N - 1);
           {Ref, Solution} ->
               Solution
       end.
   ```

   ![image-20250422163618611](./report_img/image-20250422163618611.png)

   We observed speed up differences between solving different puzzles. The solving time for the "extreme" puzzle, particularly "seventeen," increased significantly.

2. TODO: try to use pool to manage spawn?