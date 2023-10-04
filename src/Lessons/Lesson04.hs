
module Lessons.Lesson04 () where

import Control.Monad ( guard )

search :: Eq a => [(a, b)] -> a -> Maybe b
search [] _ = Nothing
search ((k,v):xs) key =
    if key == k then Just v else search xs key

data Train = Train Int

instance Eq Train where
    (Train intA) == (Train intB) = intA == intB

class Mazdaug a where
    (~=~) :: a -> a -> Bool

instance Mazdaug Train where
  (Train intA) ~=~ (Train intB) = intA ~=~ intB

instance Mazdaug Int where
  a ~=~ b = abs (a - b) < 2

mazdaugSearch :: Mazdaug a => [(a, b)] -> a -> Maybe b
mazdaugSearch [] _ = Nothing
mazdaugSearch ((k,v):xs) key =
    if key ~=~ k then Just v else mazdaugSearch xs key

lh1 :: [(Integer, Char)]
lh1 = [(a, b) | a <- [1 .. 5], b <- ['a' .. 'z']]


lh2 :: [(Integer, Integer)]
lh2 = [(a, b) | a <- [1 .. 5], b <- [a .. 9]]

lh3 :: [(Integer, Integer)]
lh3 = [(a, b) | a <- [1 .. 5], b <- [a .. 9], _ <- [1,2]]

lh4 :: [(Integer, Integer)]
lh4 = [(a, b) | a <- [1 .. 5], b <- [a .. 9], a /= b]

lh5 :: [Integer]
lh5 = [1 | a <- [1 .. 5], b <- [a .. 9], c <- [1,2]]

lh6 :: [(Integer, Integer)]
lh6 = [(a, b) | a <- [1 .. 5], b <- [a .. 9], _ <- []]

dn1 :: [(Integer, Integer)]
dn1 = do 
    a <- [1 .. 5]
    b <- [a .. 9]
    return (a, b)

dn2 :: [(Integer, Integer)]
dn2 = do 
    a <- [1 .. 5]
    b <- []
    return (a, b)

dn3 :: [(Integer, Integer)]
dn3 = do 
    a <- [1]
    b <- [2]
    return (a, b)

dn4 :: Maybe (Integer, Integer)
dn4 = do 
    a <- Just 1
    b <- Just 2
    return (a, b)

dn5 :: Maybe (Integer, Integer)
dn5 = do 
    a <- Just 1
    b <- Nothing
    return (a, b)

dn6 :: Maybe String
dn6 = do 
    a <- Nothing
    b <- Just "A"
    return b

dn7 :: Maybe String
dn7 = do 
    a <- Just 'a'
    b <- Just "A"
    return b

dn8 :: Maybe String
dn8 = do 
    a <- Just 'a'
    b <- Just "A"
    return (a:b)

dn9 :: Either String Int
dn9  = do 
    a <- Right 42
    b <- Right 2
    return (a*b)

dn10 :: Either String Int
dn10  = do 
    a <- Right 42
    b <- Left "oops"
    return (a*b)

dn11 :: Either String Int
dn11  = do 
    a <- Right 42
    b <- Left "oops"
    return (a*b)

dn12 :: [(Integer, Integer)]
dn12 = do 
    a <- [1]
    b <- [2]
    [(a, b)]

dn13 :: Maybe String
dn13 = do 
    a <- Just 'a'
    b <- Just "A"
    Just (a:b)
