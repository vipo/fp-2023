module Lessons.Lesson01 () where

x :: Integer
x = 5

b :: Bool
b = True

s :: String
s = "osefwiefpoij"

add :: Integer -> Integer -> Integer
add a b = a + b

aTuple :: (Integer, String)
aTuple = (42, "Hellou")

add' :: (Integer, Integer) -> Integer
add' (f, s) = f + s


l :: [Integer]
l = [42, 123]

l1 :: [Integer]
l1 = 1 : l


leng :: [a] -> Integer
leng [] = 0
leng (_:xs) = 1 + leng xs

-- leng [1, 2, 3]
-- (1 + leng [2, 3])
-- (1 + (1 + leng [3]))
-- (1 + (1 + (1 + leng [])))
-- (1 + (1 + (1 + 0)))