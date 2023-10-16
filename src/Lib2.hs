{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE DataKinds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement (..)
  )
where

import DataFrame (DataFrame, Value)
import InMemoryTables (TableName)
import Control.Applicative ( Alternative(empty, (<|>)), optional )
import Data.Char (toLower, isSpace, isAlphaNum)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]
type ColumnName = String

data RelationalOperator
    = EQ
    | NE
    | LT
    | GT
    | LE
    | GE
    deriving (Show, Eq)

data LogicalOperator
    = And
    deriving (Show, Eq)

data Expression
    = ValueExpression Value
    | ColumnExpression ColumnName
    deriving (Show, Eq)

data WhereCriterion = WhereCriterion ColumnName RelationalOperator Expression
    deriving (Show, Eq)

data AggregateFunction
    = Min
    | Sum
    deriving (Show, Eq)

data Aggregate = Aggregate AggregateFunction ColumnName
    deriving (Show, Eq)

data SelectData
    = SelectColumn ColumnName
    | SelectAggregate Aggregate
    deriving (Show, Eq)

type SelectQuery = [SelectData]
type WhereClause = [(WhereCriterion, Maybe LogicalOperator)]

-- Keep the type, modify constructors
data ParsedStatement = SelectStatement {
    table :: TableName,
    query :: SelectQuery,
    whereClause :: WhereClause
} | ShowTableStatement {
    table :: TableName
} | ShowTablesStatement { }
    deriving (Show, Eq)

newtype Parser a = Parser {
    runParser :: String -> Either ErrorMessage (String, a)
}

instance Functor Parser where
    fmap :: (a -> b) -> Parser a -> Parser b
    fmap f p = Parser $ \inp ->
        case runParser p inp of
            Left err -> Left err
            Right (l, a) -> Right (l, f a)

instance Applicative Parser where
    pure :: a -> Parser a
    pure a = Parser $ \inp -> Right (inp, a)
    (<*>) :: Parser (a -> b) -> Parser a -> Parser b
    pf <*> pa = Parser $ \inp1 ->
        case runParser pf inp1 of
            Left err1 -> Left err1
            Right (inp2, f) -> case runParser pa inp2 of
                Left err2 -> Left err2
                Right (inp3, a) -> Right (inp3, f a)

instance Alternative Parser where
    empty :: Parser a
    empty = Parser $ \_ -> Left "Error"
    (<|>) :: Parser a -> Parser a -> Parser a
    p1 <|> p2 = Parser $ \inp ->
        case runParser p1 inp of
            Right a1 -> Right a1
            Left _ -> case runParser p2 inp of
                Right a2 -> Right a2
                Left err -> Left err

instance Monad Parser where
    (>>=) :: Parser a -> (a -> Parser b) -> Parser b
    pa >>= pbGen = Parser $ \inp1 ->
        case runParser pa inp1 of
            Left err1 -> Left err1
            Right (inp2, a) -> case runParser (pbGen a) inp2 of
                Left err2 -> Left err2
                Right (inp3, b) -> Right (inp3, b)

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement inp = case runParser parser (dropWhile isSpace inp) of
    Left err1 -> Left err1
    Right (rest, statement) -> case runParser parseEndOfStatement rest of
        Left err2 -> Left err2
        Right _ -> Right statement
    where
        parser :: Parser ParsedStatement
        parser = parseShowTableStatement

parseShowTableStatement :: Parser ParsedStatement
parseShowTableStatement = do
    _ <- parseKeyword "show"
    _ <- parseWhitespace
    _ <- parseKeyword "table"
    _ <- parseWhitespace
    ShowTableStatement <$> parseWord

parseChar :: Char -> Parser Char
parseChar ch = Parser $ \inp ->
    case inp of
        [] -> Left "Empty input"
        (x:xs) -> if ch == x then Right (xs, ch) else Left ("Expected " ++ [ch])

parseWord :: Parser String
parseWord = Parser $ \inp ->
    case takeWhile (\x -> isAlphaNum x || x == '_') inp of
        [] -> Left "Empty input"
        xs -> Right (drop (length xs) inp, xs)

parseKeyword :: String -> Parser String
parseKeyword keyword = Parser $ \inp ->
    if map toLower (take len inp) == map toLower keyword then
        Right (drop len inp, keyword)
    else
        Left $ "Expected " ++ keyword
    where
        len = length keyword

parseWhitespace :: Parser String
parseWhitespace = Parser $ \inp ->
    case span isSpace inp of
        ("", _) -> Left $ "Expected whitespace before: " ++ inp
        (whitespace, rest) -> Right (rest, whitespace)

parseEndOfStatement :: Parser String
parseEndOfStatement = do
    _ <- optional parseWhitespace
    _ <- optional (parseChar ';')
    _ <- optional parseWhitespace
    ensureNothingLeft
    where
        ensureNothingLeft :: Parser String
        ensureNothingLeft = Parser $ \inp ->
            case inp of
                [] -> Right ([], [])
                _ -> Left "Characters found after end of SQL statement."

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement _ = Left "Not implemented: executeStatement"
