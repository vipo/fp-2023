{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE BlockArguments #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import DataFrame
    ( DataFrame(..),
      Row,
      Column(..),
      ColumnType(..),
      Value(..),
      DataFrame(DataFrame) )
import InMemoryTables (TableName, database)
import Data.List.NonEmpty (some1)
import Foreign.C (charIsRepresentable)
import Data.Char (toLower, GeneralCategory (ParagraphSeparator))
import qualified InMemoryTables as DataFrame
import Lib1 (renderDataFrameAsTable, findTableByName)
import Data.List (isPrefixOf)
import Data.Maybe (fromMaybe)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement = ParsedStatement
  | ShowTable String
  | ShowTables [TableName]
  | Err ErrorMessage
  deriving (Show)
--------------------------------------------------------------------------------
newtype Parser a = Parser {
    runParser :: String -> Either ErrorMessage (a, String)
}

instance Functor Parser where
  fmap f (Parser x) = Parser $ \s -> do
    (x', s') <- x s
    return (f x', s')

instance Applicative Parser where
  pure x = Parser $ \s -> Right (x, s)
  (Parser f) <*> (Parser x) = Parser $ \s -> do
    (f', s1) <- f s
    (x', s2) <- x s1
    return (f' x', s2)

instance Monad Parser where
  (Parser x) >>= f = Parser $ \s -> do
    (x', s') <- x s
    runParser (f x') s'

instance MonadFail Parser where
  fail _ = Parser $ \_ -> Left "Monad failed"

char :: Char -> Parser Char
char c = Parser charP
  where charP []                 = Left "Empty"
        charP (x:xs) | x == c    = Right (c, xs)
                     | otherwise = Left "Do not match"

string :: String -> Parser String
string = mapM char

----------------------------------------------------------------------------------

charToString :: Char -> String
charToString c = [c]

toLowerString :: String -> String
toLowerString [] = ""
toLowerString (x:xs) = charToString (toLower x) ++ toLowerString xs

findTableNames :: [TableName]
findTableNames = findTuples InMemoryTables.database

findTuples :: Database -> [TableName]
findTuples [] = []
findTuples db = map firstFromTuple db

firstFromTuple :: (TableName, DataFrame) -> TableName
firstFromTuple = fst

parserToStatement :: Parser String -> ParsedStatement
parserToStatement p = case runParser p "" of
  Right (parsedStr, _) -> ShowTable parsedStr -- Here, you should pattern match the parsed string to create a ParsedStatement
  Left err -> Err err -- Handle the case where parsing fails

parseTableName :: String -> Parser String
parseTableName str = do
  case lookup str database of
    Just _ -> string str
    Nothing -> string "Table not found in the database"

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement query
  | toLowerString query == "show tables;" = Right (ShowTables findTableNames)--(ShowTable )--(parserToStatement (string query))--Right (string query)
  | "show table" `isPrefixOf` toLowerString query = Right (parserToStatement (parseTableName (drop 11 (init query))))
  | otherwise = Left "FAILED."

-- Executes a parsed statement. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (ShowTable tableName) = Right (fromMaybe (createTablesDataFrame []) (lookup (show tableName) database))
executeStatement (ShowTables tableNames) = Right (createTablesDataFrame tableNames)
executeStatement (Err err) = Left err
--executeStatement _  = Left "Not implemented: executeStatement"
--executeStatement ps = 

createTablesDataFrame :: [TableName] -> DataFrame
createTablesDataFrame tableNames = DataFrame [Column "Tables" StringType] (map (\name -> [StringValue name]) tableNames)
