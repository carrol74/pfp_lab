import "lib/github.com/diku-dk/sorts/radix_sort"

-- 2.2
def segop 't (op     : t->t->t)
                 (x : t, flag_x : bool)
                 (y : t, flag_y : bool) : (t, bool) =
  let flag = flag_x || flag_y
  let value = if flag_y 
                then y
                else op x y
  in (value, flag)

def segscan [n] 't (op: t -> t -> t) 
                   (ne: t)
                   (arr: [n](t, bool)): *[n]t =
  let op' = segop op
  let ne' = (ne, false)
  let (res, _) = unzip <| scan op' ne' arr
  in res

def reduce_index [n] (flags: [n]bool) : [n]i64 =
  let seg_nums = scan (+) 0 (map (\f -> if f then 1 else 0) flags)
  let is_end = map (\i ->
                      if i == n-1
                      then true
                      else flags[i+1]
                   )
                   (iota n)
  in map2 (\i num -> 
              if is_end[i] 
              then num 
              else -1)
          (iota n)
          seg_nums

def segreduce [n] 't (op: t -> t -> t)
                     (ne: t)
                     (arr: [n](t, bool)): *[]t =
  let sc_arr = segscan op ne arr
  let (_, flags) = unzip arr
  let w = length (filter (\b -> b) flags)  + 1
  in  scatter (replicate w ne)
              (reduce_index flags)
              sc_arr

-- TODO: benchmarks

-- 2.3
def hist 'a [n] (op : a -> a -> a) (ne : a)
                (k: i64) (is : [n]i64) (as : [n]a) : [k]a =
  ???