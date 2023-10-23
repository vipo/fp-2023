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
      DataFrame)
import InMemoryTables (TableName, database)
import Data.List.NonEmpty (some1, xor)
import Foreign.C (charIsRepresentable)
import Data.Char (toLower, GeneralCategory (ParagraphSeparator), isSpace)
import qualified InMemoryTables as DataFrame
import Lib1 (renderDataFrameAsTable, findTableByName)
import Data.List (isPrefixOf)
import Data.Maybe (fromMaybe)
import Text.ParserCombinators.ReadP (get)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement =
  ShowTable {
    table :: TableName
   }
   | ShowTables { }
    deriving (Show, Eq)

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

class (Applicative f) => Alternative f where
  empty :: f a
  ( <|> ) :: f a -> f a -> f a
  some :: f a -> f [a]
  some v = some_v
    where many_v = some_v <|> pure []
          some_v = (:) <$> v <*> many_v

  many :: f a -> f [a]
  many v = many_v
    where many_v = some_v <|> pure []
          some_v = (:) <$> v <*> many_v

instance Alternative Parser where
  empty = fail "empty"
  (Parser x) <|> (Parser y) = Parser $ \s ->
    case x s of
      Right x -> Right x
      Left _ -> y s

char :: Char -> Parser Char
char c = Parser charP
  where charP []                 = Left "No table name was provided"
        charP (x:xs) | x == c    = Right (c, xs)
                     | otherwise = Left "Table with this name does not exist or ; is missing"

string :: String -> Parser String
string = mapM char

optional :: Parser a -> Parser (Maybe a)
optional p = do
  result <- p
  return (Just result)
  <|> return Nothing


----------------------------------------------------------------------------------

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement query = case runParser p query of
    Left err1 -> Left err1
    Right (query, rest) -> case query of
        ShowTable _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        ShowTables -> case runParser stopParseAt rest of
            Left err2 -> Left err2
            Right _ -> Right query
    where
        p :: Parser ParsedStatement
        p = showTablesParser
               <|> showTableParser

queryStatementParser :: String -> Parser String
queryStatementParser queryStatement = Parser $ \query ->
    case take (length queryStatement) query of
        [] -> Left "Empty input"
        xs
            | map toLower xs == map toLower queryStatement -> Right (xs, drop (length xs) query)
            | otherwise -> Left $ "Expected " ++ queryStatement

showTablesParser :: Parser ParsedStatement
showTablesParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "tables"
    _ <- optional whitespaceParser
    pure ShowTables

showTableParser :: Parser ParsedStatement
showTableParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    ShowTable <$> tableNameParser

tableNameParser :: Parser TableName 
tableNameParser = Parser $ \query ->
  case isValidTableName query of
    True ->
      case lookup (dropWhiteSpaces (init query)) InMemoryTables.database of
      Just _ -> Right (init (dropWhiteSpaces query), ";")
      Nothing -> Left "Table not found in the database"
    False -> Left "Query does not end with ; or contains unnecessary words after table name"

isValidTableName :: String -> Bool
isValidTableName str =
  if last str == ';' then (case isOneWord str of
  True -> True
  False -> False) else False

isOneWord :: String -> Bool
isOneWord [] = True
isOneWord (x:xs)
  | x /= ' ' = isOneWord xs
  | x == ' ' = dropWhiteSpaces xs == ";"

dropWhiteSpaces :: String -> String
dropWhiteSpaces [] = []
dropWhiteSpaces (x:xs)
  | x /= ' ' = [x] ++ dropWhiteSpaces xs
  | otherwise = dropWhiteSpaces xs

whitespaceParser :: Parser String
whitespaceParser = Parser $ \query ->
    case span isSpace query of
        ("", _) -> Left $ "Expected whitespace before " ++ query
        (rest, whitespace) -> Right (rest, whitespace)

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables = Right (createTablesDataFrame findTableNames)
executeStatement  (ShowTable table) = Right (createColumnsDataFrame (columnsToList (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database))) table)
executeStatement _ = Left "Not implemented: executeStatement for other statements"

createColumnsDataFrame :: [TableName] -> TableName -> DataFrame
createColumnsDataFrame columnNames columnTableName = DataFrame [Column columnTableName StringType] (map (\name ->  [StringValue name]) columnNames)

columnsToList :: DataFrame -> [TableName]
columnsToList (DataFrame [] []) = []
columnsToList (DataFrame columns _) = map getColumnName columns

getColumnName :: Column -> TableName
getColumnName (Column "" _) = ""
getColumnName (Column columnname _) = columnname 

findTableNames :: [TableName]
findTableNames = findTuples InMemoryTables.database

findTuples :: Database -> [TableName]
findTuples [] = []
findTuples db = map firstFromTuple db

firstFromTuple :: (TableName, DataFrame) -> TableName
firstFromTuple = fst

createTablesDataFrame :: [TableName] -> DataFrame
createTablesDataFrame tableNames = DataFrame [Column "Tables" StringType] (map (\name -> [StringValue name]) tableNames)

stopParseAt :: Parser String
stopParseAt  = do
     _ <- optional whitespaceParser
     _ <- queryStatementParser ";"
     checkAfterQuery
     where
        checkAfterQuery :: Parser String
        checkAfterQuery = Parser $ \query ->
            case query of
                [] -> Right ([], [])
                s -> Left ("Characters found after ;" ++ s)
