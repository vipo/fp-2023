{-# OPTIONS_GHC -Wno-noncanonical-monoid-instances #-}
module Lessons.Lesson14 () where
import Control.Monad.Trans.Iter (fold)

import Control.DeepSeq (deepseq)

data B = B !Int !Int deriving (Eq, Show)

instance Monoid B where
  mempty :: B
  mempty = B 0 0

instance Semigroup B where
   (<>) :: B -> B -> B
   (<>) (B a b) (B c d)
    | b < c = B (a + c - b) d
    | otherwise = B a (d + b - c)

parse :: Char -> B
parse '(' = B 0 1
parse ')' = B 1 0
parse _ = B 0 0

balance :: Foldable t => t Char -> B
balance xs = foldMap parse xs