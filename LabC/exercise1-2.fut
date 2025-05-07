-- 1.1
entry process [n] (xs: [n]i32) (ys: [n]i32): i32 =
  reduce i32 . max 0 (map i32 . abs (map2 (-) xs ys ))

-- 1.2

-- Test the process function.
-- ==
-- entry: process
-- input { [23,45,-23,44,23,54,23,12,34,54,7,2,4,67]
--         [-2,3,4,57,34,2,5,56,56,3,3,5,77,89] }
-- output { 73 }

-- 1.3
-- ==
-- input @ two_100_i32s
-- input @ two_1000_i32s
-- input @ two_10000_i32s
-- input @ two_100000_i32s
-- input @ two_1000000_i32s
-- input @ two_5000000_i32s
-- input @ two_10000000_i32s
-- def main ( xs : [] i32 ) ( ys : [] i32 ) = process xs ys

-- 1.4
entry process_idx [n] (xs: [n]i32) (ys: [n]i32): (i32, i64) =
  reduce (\(x, i) (y, j) -> 
              if      x > y then (x,i)
              else if y > x then (y,j)
              else if i > j then (x,i)
              else               (y,j))
           (0, -1)
           (zip (map i32 . abs (map2 (-) xs ys )) (iota n))

-- ==
-- input @ two_100_i32s
-- input @ two_1000_i32s
-- input @ two_10000_i32s
-- input @ two_100000_i32s
-- input @ two_1000000_i32s
-- input @ two_5000000_i32s
-- input @ two_10000000_i32s

def main (xs: []i32) (ys: []i32) = process_idx xs ys
