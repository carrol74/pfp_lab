import Data.List
import System.Random
import Criterion.Main
import Control.Parallel
import Control.Parallel.Strategies
import Control.Monad.Par

-- code borrowed from the Stanford Course 240h (Functional Systems in Haskell)
-- I suspect it comes from Bryan O'Sullivan, author of Criterion

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

main = do
  let (xs,ys) = splitAt 1500  (take 6000
                               (randoms (mkStdGen 211570155)) :: [Float] )
  -- handy (later) to give same input different parallel functions

  let rs = crud xs ++ ys
  putStrLn $ "sample mean:    " ++ show (mean rs)

  let j = jackknife mean rs :: [Float]
  putStrLn $ "jack mean min:  " ++ show (minimum j)
  putStrLn $ "jack mean max:  " ++ show (maximum j)
  defaultMain
        [
         bench "jackknife" (nf (jackknife  mean) rs),
         bench "jackknife par" (nf (parJackknife parMapA mean) rs),
         bench "jackknife Eval Monad" (nf (parJackknife parMapEval mean) rs),
         bench "jackknife Built-in" (nf (parJackknife parMapBuiltIn mean) rs),
         bench "jackknife Strategy" (nf (parJackknife parMapStrategy mean) rs),
         bench "jackknife Par Monad" (nf (parJackknife parMapPar mean) rs)
         ]