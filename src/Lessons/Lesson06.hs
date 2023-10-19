{-# LANGUAGE InstanceSigs #-}
module Lessons.Lesson06 () where

import Control.Applicative((<|>), empty, Alternative (some, many))

import Data.Char(isAlphaNum, toLower)
import DataFrame (Column(Column))
import GHC.Conc (par)

dn10 :: Maybe Int
dn10  = do 
    a <- Just 42
    b <- Just 2
    return (a*b)

dn20 :: Maybe Int
dn20 = Just 42 >>= (\a -> Just 2 >>= (\b -> return (a*b)))

dn11 :: Either String Int
dn11  = do 
    a <- Right 42
    b <- Left "oops"
    return (a*b)

dn21 :: Either String Int
dn21  = Right 42 >>= (\a -> Left "oops" >>= (\b -> return (a*b)))

type ErrorMessage = String

newtype Parser a = Parser {
    runParser :: String -> Either ErrorMessage (String, a)
}

instance Functor Parser where
  fmap :: (a -> b) -> Parser a -> Parser b
  fmap f functor = Parser $ \inp ->
    case runParser functor inp of
        Left e -> Left e
        Right (l, a) -> Right (l, f a)

instance Applicative Parser where
  pure :: a -> Parser a
  pure a = Parser $ \inp -> Right (inp, a)
  (<*>) :: Parser (a -> b) -> Parser a -> Parser b
  ff <*> fa = Parser $ \in1 ->
    case runParser ff in1 of
        Left e1 -> Left e1
        Right (in2, f) -> case runParser fa in2 of
            Left e2 -> Left e2
            Right (in3, a) -> Right (in3, f a)

instance Alternative Parser where
  empty :: Parser a
  empty = Parser $ \_ -> Left "Error"
  (<|>) :: Parser a -> Parser a -> Parser a
  p1 <|> p2 = Parser $ \inp ->
    case (runParser p1) inp of
        Right r1 -> Right r1
        Left _ -> case runParser p2 inp of
            Right r2 -> Right r2
            Left e -> Left e

instance Monad Parser where
  (>>=) :: Parser a -> (a -> Parser b) -> Parser b
  ma >>= mf = Parser $ \inp1 ->
    case runParser ma inp1 of
        Left e1 -> Left e1
        Right (inp2, a) -> case runParser (mf a ) inp2 of
            Left e2 -> Left e2
            Right (inp3, r) -> Right (inp3, r)

parseName :: Parser String
parseName = Parser $ \inp ->
    case takeWhile isAlphaNum inp of
        [] -> Left "Empty input"
        xs -> Right (drop (length xs) inp, xs)

parseChar :: Char -> Parser Char
parseChar a = Parser $ \inp ->
    case inp of
        [] -> Left "Empty input"
        (x:xs) -> if a == x then Right (xs, a) else Left ([a] ++ " expected but " ++ [x] ++ " found")

data Columns = All | ColumntList [String] deriving Show

parseColumns :: Parser Columns
parseColumns = parseAll <|> parseColumnList

parseAll :: Parser Columns
parseAll = fmap (\_ -> All) $ parseChar '*'

parseColumnList :: Parser Columns
parseColumnList = do
    name <- parseName
    other <- many parseCSName
    return $ ColumntList $ name:other

parseCSName :: Parser String
parseCSName = do
    _ <- many $ parseChar ' '
    _ <- parseChar ','
    _ <- many $ parseChar ' '
    name <- parseName
    return name

parseQuery :: String -> Either ErrorMessage (String, Columns)
parseQuery inp = runParser parseColumns inp

parseKeyword :: String -> Parser String
parseKeyword keyword = Parser $ \inp ->
  if map toLower (take l inp) == map toLower keyword then
    Right (drop l inp, keyword)
  else
    Left $ keyword ++ " expected"
  where
    l = length keyword

data Student = Student {
    name :: String,
    surn :: String
} deriving Show