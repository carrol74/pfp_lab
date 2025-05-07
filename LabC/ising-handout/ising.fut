-- We represent a spin as a single byte.  In principle, we need only
-- two values (-1 or 1), but Futhark represents booleans a a full byte
-- entirely, so using an i8 instead takes no more space, and makes the
-- arithmetic simpler.
type spin = i8

import "lib/github.com/diku-dk/cpprandom/random"

-- Pick an RNG engine and define random distributions for specific types.
module rng_engine = minstd_rand
module rand_f32 = uniform_real_distribution f32 rng_engine
module rand_i8 = uniform_int_distribution i8 rng_engine

-- We can create an few RNG state with 'rng_engine.rng_from_seed [x]',
-- where 'x' is some seed.  We can split one RNG state into many with
-- 'rng_engine.split_rng'.
--
-- For an RNG state 'r', we can generate random integers that are
-- either 0 or 1 by calling 'rand_i8.rand (0i8, 1i8) r'.
--
-- For an RNG state 'r', we can generate random floats in the range
-- (0,1) by calling 'rand_f32.rand (0f32, 1f32) r'.
--
-- Remember to consult
-- https://futhark-lang.org/pkgs/github.com/diku-dk/cpprandom/latest/

def rand = rand_f32.rand (0f32, 1f32)

-- Create a new grid of a given size.  Also produce an identically
-- sized array of RNG states.
def random_grid (seed: i32) (h: i64) (w: i64)
              : ([h][w]rng_engine.rng, [h][w]spin) =
  let initial_rng = rng_engine.rng_from_seed [seed]
  let rngs_flat = rng_engine.split_rng (h*w) initial_rng
  
  -- Reshape into h×w grid
  let rngs = unflatten rngs_flat
  -- For each RNG state, generate a random spin (-1 or 1)
  let spins = map (\row ->
                    map (\r ->
                          -- Generate random i8 that is either 0 or 1
                          let (_, i) = rand_i8.rand (0i8, 1i8) r
                          -- Transform to -1 or 1 by: (i*2)-1
                          in (i*2i8)-1i8
                        ) row
                  ) rngs
                  
  -- Return the tuple of RNG states and spins
  in (rngs, spins)

-- Compute $\Delta_e$ for each spin in the grid, using wraparound at
-- the edges.
def deltas [h][w] (spins: [h][w]spin): [h][w]i8 =
  let up = rotate (-1) spins       -- Rotate grid up (row above)
  let down = rotate 1 spins        -- Rotate grid down (row below)
  let left = map (rotate (-1)) spins  -- Rotate each row left
  let right = map (rotate 1) spins    -- Rotate each row right
  
  -- Calculate delta_e = 2c(u + d + l + r) for each position
  in map5 (\row_c row_u row_d row_l row_r ->
            map5 (\c u d l r ->
                   2i8 * c * (u + d + l + r)
                 ) row_c row_u row_d row_l row_r
          ) spins up down left right

-- The sum of all deltas of a grid.  The result is a measure of how
-- ordered the grid is.
def delta_sum [h][w] (spins: [w][h]spin): i32 =
   -- Calculate the deltas
  let ds = deltas spins
  -- Flatten the 2D array to 1D
  let flat_ds = flatten ds
  -- Sum all values and convert to i32
  in i32.sum (map i32.i8 flat_ds)

-- Take one step in the Ising 2D simulation.
def step [h][w] (abs_temp: f32) (samplerate: f32)
                (rngs: [h][w]rng_engine.rng) (spins: [h][w]spin)
              : ([h][w]rng_engine.rng, [h][w]spin) =
   let ds = deltas spins
  
  -- Generate two random numbers for each cell and update RNG states
  -- First set of random numbers (a)
  let (rngs', as) = unzip (map (\row_rngs ->
                               unzip (map rand row_rngs))
                           rngs)
  
  -- Second set of random numbers (b)
  let (rngs'', bs) = unzip (map (\row_rngs ->
                                unzip (map rand row_rngs))
                            rngs')
  
  -- Update each spin based on the model rules
  let spins' = map4 (\row_spins row_ds row_as row_bs ->
                      map4 (\spin d a b ->
                             -- A spin flips if:
                             -- 1. a < samplerate (determines if this spin is a candidate for flipping)
                             -- 2. AND (d < 0 OR b < e^(-d/abs_temp))
                             -- Where d < 0 means flipping would reduce energy
                             let flip_condition = 
                               a < samplerate && 
                               (d < 0i8 || b < f32.exp(f32.i8(-d) / abs_temp))
                             in if flip_condition
                                then -spin  -- Flip the spin
                                else spin   -- Keep the spin unchanged
                           ) row_spins row_ds row_as row_bs
                    ) spins ds as bs
  
  in (rngs'', spins')

-- | Just for benchmarking.
def main (abs_temp: f32) (samplerate: f32)
         (h: i64) (w: i64) (n: i32): [h][w]spin =
  (loop (rngs, spins) = random_grid 1337 h w for _i < n do
     step abs_temp samplerate rngs spins).1

-- ==
-- entry: main
-- input { 0.5f32 0.1f32 10i64 10i64 2 } auto output

-- The following definitions are for the visualisation and need not be modified.

type~ state = {cells: [][](rng_engine.rng, spin)}

entry tui_init seed h w : state =
  let (rngs, spins) = random_grid seed h w
  in {cells=map (uncurry zip) (zip rngs spins)}

entry tui_render (s: state) = map (map (.1)) s.cells

entry tui_step (abs_temp: f32) (samplerate: f32) (s: state) : state =
  let rngs = (map (map (.0)) s.cells)
  let spins = map (map (.1)) s.cells
  let (rngs', spins') = step abs_temp samplerate rngs spins
  in {cells=map (uncurry zip) (zip rngs' spins')}
