{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE BlockArguments #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE DataKinds #-}
{-# OPTIONS_GHC -Wno-overlapping-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Eta reduce" #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# HLINT ignore "Redundant return" #-}
{-# OPTIONS_GHC -Wno-orphans #-}


module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement (..),
    queryStatementParser,
    whitespaceParser,
    showTablesParser,
    showTableParser,
    tableNameParser,
    isValidTableName,
    isOneWord,
    dropWhiteSpaces,
    columnsToList,
    getColumnName,
    findTableNames,
    findTuples,
    firstFromTuple,
    selectStatementParser,
    columnNamesParser,
    areColumnsListedRight,
    splitStatementAtFrom,
    split,
    toLowerString,
    charToString,
    createColumnsDataFrame,
    createColumnsDataFrame,
    stopParseAt
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
import Data.Foldable (find)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

type ColumnName = String

-- Keep the type, modify constructors
data ParsedStatement =
  Select {
    column :: [ColumnName],
    table :: TableName
  }
  | ShowTable {
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
        Select _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
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
               <|> selectStatementParser


executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables = Right $ createTablesDataFrame findTableNames
executeStatement  (ShowTable table) = Right (createColumnsDataFrame (columnsToList (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database))) table)
--executeStatement Select = Right 
executeStatement _ = Left "Not implemented: executeStatement for other statements"
---------------------------------------------------------------------------------

queryStatementParser :: String -> Parser String   
queryStatementParser queryStatement = Parser $ \query ->
    case take (length queryStatement) query of
        [] -> Left "Expected ;"
        xs
            | map toLower xs == map toLower queryStatement -> Right (xs, drop (length xs) query)
            | otherwise -> Left $ "Expected " ++ queryStatement ++ " or query contains unnecessary words"

whitespaceParser :: Parser String
whitespaceParser = Parser $ \query ->
    case span isSpace query of
        ("", _) -> Left $ "Expected whitespace before " ++  query
        (rest, whitespace) -> Right (rest, whitespace)

-------------------------------------------------------------------------------------

showTablesParser :: Parser ParsedStatement
showTablesParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "tables"
    _ <- optional whitespaceParser
    pure ShowTables

------------------------------------------------------------------------------------
 
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
      Nothing -> Left "Table not found in the database or not provided"
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

columnsToList :: DataFrame -> [ColumnName]
columnsToList (DataFrame [] []) = []
columnsToList (DataFrame columns _) = map getColumnName columns

getColumnName :: Column -> ColumnName
getColumnName (Column "" _) = ""
getColumnName (Column columnname _) = columnname 

findTableNames :: [ColumnName]
findTableNames = findTuples InMemoryTables.database

findTuples :: Database -> [ColumnName]
findTuples [] = []
findTuples db = map firstFromTuple db

firstFromTuple :: (ColumnName, DataFrame) -> ColumnName
firstFromTuple = fst

--SELECT flag, value FROM flags;

selectStatementParser :: Parser ParsedStatement
selectStatementParser = do
    _ <- queryStatementParser "select"
    _ <- whitespaceParser
    columns <- columnNamesParser
    _ <- whitespaceParser
    _ <- queryStatementParser "from"
    _ <- whitespaceParser
    Select columns <$> tableNameParser


columnNamesParser :: Parser [ColumnName]
columnNamesParser = Parser $ \query ->
  case query == "" || (dropWhiteSpaces query) == ";" of
    True -> Left "Column name is expected"
    False -> case toLowerString(head(split query ' ')) == "from" of
      True -> Left "No column name was provided"
      False -> case areColumnsListedRight (fst (splitStatementAtFrom query)) && areColumnsListedRight (snd (splitStatementAtFrom query)) of
        True ->Right ((split (dropWhiteSpaces(fst (splitStatementAtFrom query))) ','), snd (splitStatementAtFrom query))
        False -> Left "Column names are not listed right or from is missing"

areColumnsListedRight :: String -> Bool --zodis -> whitespace rekursija -> kablelis -> whitespace rekursija -> zodis -> from
areColumnsListedRight str
  | str == "" = False
  | last (dropWhiteSpaces str) == ','  || head (dropWhiteSpaces str) == ',' = False
  | otherwise = True

splitStatementAtFrom :: String -> (String, String)
splitStatementAtFrom = go [] where
  go _ [] = ("", "")
  go prefix str@(x:xs)
    | " from" `isPrefixOf` toLowerString str = (reverse prefix, str)
    | otherwise = go (x:prefix) xs

split :: String -> Char -> [String]
split [] _ = [""]
split (c:cs) delim
    | c == delim = "" : rest
    | otherwise = (c : head rest) : tail rest
    where
        rest = split cs delim

toLowerString :: String -> String
toLowerString [] = ""
toLowerString (x:xs) = charToString (toLower x) ++ toLowerString xs

charToString :: Char -> String
charToString c = [c]

----------------------------------------------------------------------------------------------------------

createColumnsDataFrame :: [ColumnName] -> TableName -> DataFrame
createColumnsDataFrame columnNames columnTableName = DataFrame [Column columnTableName StringType] (map (\name ->  [StringValue name]) columnNames)

createTablesDataFrame :: [TableName] -> DataFrame
createTablesDataFrame tableNames = DataFrame [Column "Tables" StringType] (map (\name -> [StringValue name]) tableNames)

---------------------------------------------------------------------------------------------------------

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
