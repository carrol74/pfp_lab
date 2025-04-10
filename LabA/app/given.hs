import Data.List
import System.Random
import Criterion.Main
import Control.Parallel
import Control.Parallel.Strategies
import Control.Monad.Par
import Control.DeepSeq --need deepseq for force function --> needed for sorting algorithms due to lists being lazy structures


-- code borrowed from the Stanford Course 240h (Functional Systems in Haskell)
-- I suspect it comes from Bryan O'Sullivan, author of Criterion


-- Assignment 1
data T a = T !a !Int


mean :: (RealFrac a) => [a] -> a
mean = fini . foldl' go (T 0 0)
  where
    fini (T a _) = a
    go (T m n) x = T m' n'
      where m' = m + (x - m) / fromIntegral n'
            n' = n + 1


resamples :: Int -> [a] -> [[a]]
resamples k xs =
    take (length xs - k) $
    zipWith (++) (inits xs) (map (drop k) (tails xs))


jackknife :: ([a] -> b) -> [a] -> [b]
jackknife f = map f . resamples 500

parJackknife :: (([a] -> b) -> [[a]] -> [b]) -> ([a] -> b) -> [a] -> [b]
parJackknife parMapf f = parMapf f . resamples 500

-- A
parMapA :: (a -> b) -> [a] -> [b]
parMapA _ []     = []
parMapA f (x:xs) = parMap1 `par` parMap2 `pseq` (parMap1 : parMap2)
  where parMap1  = f x
        parMap2  = parMapA f xs

-- B
parMapEval :: (a -> b) -> [a] -> [b]
parMapEval f xs = runEval $ go xs
  where
    go [] = return []
    go (y:ys) = do
      y' <- rpar (f y)
      ys' <- go ys
      _ <- rseq y'
      return (y':ys')

parMapBuiltIn :: (a -> b) -> [a] -> [b]
parMapBuiltIn = Control.Parallel.Strategies.parMap rpar

-- C

parMapStrategy :: (a -> b) -> [a] -> [b]
parMapStrategy f xs = map f xs `using` parList rseq

-- D
parMapPar :: NFData b => (a -> b) -> [a] -> [b]
parMapPar f xs = runPar $ do
    as <- mapM (spawn . return . f) xs
    mapM get as

crud = zipWith (\x a -> sin (x / 300)**2 + a) [0..]


-- Assignment 2 (Mergesort and Quicksort)

-- | Divide and Conquer skeleton using Par Monad (Generic for MergeSort)
divConq :: (NFData b) =>
           (a -> Bool)         -- ^ isTrivial
        -> (a -> Bool)         -- ^ belowThreshold
        -> (a -> (a, a))       -- ^ divide
        -> (a -> b)            -- ^ solve
        -> ((b, b) -> b)       -- ^ combine
        -> a                   -- ^ input
        -> b

divConq isTrivial belowThreshold divide solve combine input
  | isTrivial input = solve input
  | belowThreshold input = let (l, r) = divide input in combine (solve l, solve r)
  | otherwise = runPar $ do
      let (l, r) = divide input
      let lv = force (divConq isTrivial belowThreshold divide solve combine l)
      let rv = force (divConq isTrivial belowThreshold divide solve combine r)
      iv <- spawn (return lv)
      jv <- spawn (return rv)
      lv' <- get iv
      rv' <- get jv
      return $ combine (lv', rv')

-- Sequential merge sort
mergeSort :: Ord a => [a] -> [a]
mergeSort xs
  | length xs < 2 = xs
  | otherwise     = merge (mergeSort ys) (mergeSort zs)
  where
    (ys, zs) = splitAt (length xs `div` 2) xs

-- Parallel merge sort using generic divConq
parMergeSort :: (NFData a, Ord a) => Int -> [a] -> [a]
parMergeSort n = divConq
  (\xs -> length xs < 2)                      -- trivial
  (\xs -> length xs < n)                      -- below threshold
  (\xs -> splitAt (length xs `div` 2) xs)     -- divide
  mergeSort                                   -- solve
  (\(l, r) -> merge l r)                      -- combine

-- Merge helper
merge :: Ord a => [a] -> [a] -> [a]
merge [] ys = ys
merge xs [] = xs
merge (x:xs) (y:ys)
  | x <= y    = x : merge xs (y:ys)
  | otherwise = y : merge (x:xs) ys

-- Divide and Conquer skeleton for QuickSort (note: given skeleton didnt really work due to the differences in how the original list is divided)
divConqQuick :: (NFData a, Ord a)
             => Int            -- ^ threshold
             -> ([a] -> [a])   -- ^ solve sequentially
             -> [a]            -- ^ input
             -> [a]
  
divConqQuick _ solve []  = []
divConqQuick _ solve [x] = [x]
divConqQuick n solve (x:xs)
  | length xs < n = solve (x:xs)
  | otherwise = runPar $ do
      let lesser  = filter (<= x) xs -- note x is used as pivot and hence 2 lists are made based on the pivot
      let greater = filter (> x) xs
      let left  = force (divConqQuick n solve lesser)
      let right = force (divConqQuick n solve greater)
      iv <- spawn (return left)
      jv <- spawn (return right)
      l <- get iv
      r <- get jv
      return (l ++ [x] ++ r)

-- Sequential quicksort
quickSort :: Ord a => [a] -> [a]
quickSort []     = []
quickSort (x:xs) = quickSort lesser ++ [x] ++ quickSort greater
  where
    lesser  = filter (<= x) xs
    greater = filter (> x) xs

-- Parallel quicksort using divConqQuick
parQuickSort :: (NFData a, Ord a) => Int -> [a] -> [a]
parQuickSort threshold = divConqQuick threshold quickSort



main = do

--Assignment 1 Main
  let (xs,ys) = splitAt 1500  (take 6000
                               (randoms (mkStdGen 211570155)) :: [Float] )
  -- handy (later) to give same input different parallel functions

  let rs = crud xs ++ ys
  putStrLn $ "sample mean:    " ++ show (mean rs)

  let j = jackknife mean rs :: [Float]
  putStrLn $ "jack mean min:  " ++ show (minimum j)
  putStrLn $ "jack mean max:  " ++ show (maximum j)

-- Assignment 2 Main
  let threshold = 5000
  let input = take 100000 (randoms (mkStdGen 211570155)) :: [Int]

--Benchmarking
  defaultMain
        [
         bench "jackknife" (nf (jackknife  mean) rs),
         bench "jackknife par" (nf (parJackknife parMapA mean) rs),
         bench "jackknife Eval Monad" (nf (parJackknife parMapEval mean) rs),
         bench "jackknife Built-in" (nf (parJackknife parMapBuiltIn mean) rs),
         bench "jackknife Strategy" (nf (parJackknife parMapStrategy mean) rs),
         bench "jackknife Par Monad" (nf (parJackknife parMapPar mean) rs),
         bench "Sequential MergeSort" (nf mergeSort input),
         bench "Parallel MergeSort (divConq)" (nf (parMergeSort threshold) input), 
         bench "Sequential QuickSort" (nf quickSort input), 
         bench "Parallel QuickSort (divConqQuick)" (nf (parQuickSort threshold) input)
         ]