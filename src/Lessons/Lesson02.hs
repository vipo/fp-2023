module Lessons.Lesson02 () where

leng :: [a] -> Integer
leng a = leng' a 0
    where
        leng' :: [a] -> Integer -> Integer
        leng' [] a' = a'
        leng' (_:xs) a' = leng' xs (a' + 1)

suma :: Num a => [a] -> a
suma [] = 0
suma (x:xs) = x + (suma xs)


suma' :: Num a => [a] -> a
suma' a = 
    let
        suma'' :: Num a => [a] -> a -> a
        suma'' [] acc = acc
        suma'' (x:xs) acc = suma'' xs (acc + x)
    in suma'' a 0

data Locomotive = Electric | Coal | Diesel deriving Show
data Train = Train Locomotive Integer deriving Show

data Employee = Conductor {
    id :: String
} | Engineer {
    name :: String,
    surname :: String
} deriving Show

data Railways = Railways {
    trains :: [Train],
    employees :: [Employee]
} deriving Show

r :: Railways
r = Railways [Train Electric 1, Train Coal 2, Train Diesel 0] [c, e]

c = Conductor "kazkox"
e = Engineer "Vi" "Po"

howManyTrain :: Railways -> Integer
howManyTrain (Railways t _) = leng t

howManyTrain' :: Railways -> Integer
howManyTrain' r = leng ( trains r)