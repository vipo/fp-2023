{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    ParsedStatement (..),
  )
where

import DataFrame (DataFrame)
import InMemoryTables (TableName)
import Data.Char (toLower, isAlphaNum)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

data ColumnName = ColumnName String (Maybe Condition)
  deriving (Show, Eq)

data Condition
  = Equals ColumnName String
  | NotEquals ColumnName String
  | LessThan ColumnName String
  | GreaterThan ColumnName String
  | Min
  | Avg
  deriving (Show, Eq)

data ParsedStatement
  = ShowTables
  | ShowTable String
  | Select [ColumnName] TableName [Condition]
  deriving (Show, Eq)

-- Parse a string into a ParsedStatement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input = case words input of
  [] -> Left "No statement was given"
  (firstWord : rest) -> case map toLower firstWord of
    "show" -> parseShowStatement rest
    "select" -> parseSelectStatement rest
    _ -> Left "Invalid statement: the statement should start with the keyword SHOW or SELECT"

parseShowStatement :: [String] -> Either ErrorMessage ParsedStatement
parseShowStatement [] = Left "Invalid show statement: after SHOW keyword no keyword TABLE or TABLES was found"
parseShowStatement (secondWord : remaining) = case map toLower secondWord of
  "tables" -> Right ShowTables
  "table" -> case remaining of
    [tableName] -> Right (ShowTable tableName)
    _ -> Left "Invalid show statement: there can only be one table"
  _ -> Left "Invalid show statement: after SHOW keyword no keyword TABLE or TABLES was found"


parseSelectStatement :: [String] -> Either ErrorMessage ParsedStatement
parseSelectStatement input =
  let (columns, rest) = span (not . isFrom) input
      fromKeyword = map toLower (if null rest then "" else rest !! 0)
      whereKeyword = map toLower (rest !! 2)
      conditions = drop 3 rest  
      tableName = rest !! 1
  in
  if fromKeyword == "from"
    then do
      parsedColumns <- parseColumns  (unwords columns) 
      if length rest == 2
        then
          Right (Select parsedColumns tableName [])
      else if whereKeyword == "where" && (length rest) >= 6 
        then do
          parsedConditions <- parseConditions conditions parsedColumns
          Right (Select parsedColumns tableName parsedConditions)
      else
        Left "Invalid select statement: the keyword WHERE is not writen in the appropriate position or the condition in the WHERE clause is not valid" 
    else
      Left "Invalid select statement: the keyword FROM is not writen in the appropriate position"

isFrom :: String -> Bool
isFrom word = map toLower word == "from"








parseColumns :: String -> Either ErrorMessage [ColumnName]
parseColumns input = do
  let colNames = splitColNames input
  colNameStructures <- traverse constructColumnName (map words colNames)
  return colNameStructures

splitColNames :: String -> [String]
splitColNames input = customSplit ',' input 

customSplit :: Char -> String -> [String]
customSplit _ [] = [""]
customSplit delimiter (x:xs)
    | x == delimiter = "" : rest
    | otherwise = (x : head rest) : tail rest
  where
    rest = customSplit delimiter xs

constructColumnName :: [String] -> Either ErrorMessage ColumnName
constructColumnName [col]
    | (containsOnlyLettersAndNumbers col) = Right (ColumnName col Nothing)
    | otherwise = Left "the column name contains symbols that are not allowed"
constructColumnName [agg, col]
    | lowerAgg == "min" && containsOnlyLettersAndNumbers col = Right (ColumnName col (Just Min))
    | lowerAgg == "avg" && containsOnlyLettersAndNumbers col = Right (ColumnName col (Just Avg))
    | otherwise = Left "the column name or aggregate function contains symbols that are not allowed"
  where
    lowerAgg = map toLower agg
constructColumnName _ = Left "Invalid column name structure"

containsOnlyLettersAndNumbers :: String -> Bool
containsOnlyLettersAndNumbers = all isAlphaNum






parseConditions :: [String] -> [ColumnName] -> Either ErrorMessage [Condition]
parseConditions [] columns = Right []
parseConditions input columns = 
    case break (\x -> (map toLower x) == "and") input of
      (conditionTokens, andKeyword : rest) -> do
        condition <- parseToken conditionTokens columns
        conditions <- parseConditions rest columns
        return (condition : conditions)
      _ -> do
        condition <- parseToken input columns
        return [condition]

parseToken :: [String] -> [ColumnName] -> Either ErrorMessage Condition
parseToken (colName : op : value : []) parsedColumns
    | not (matchesAnyInList colName parsedColumns) && not (matchesAnyInList value parsedColumns) = Left "Invalid column name inside a condition"
    | (matchesAnyInList colName parsedColumns) && (matchesAnyInList value parsedColumns) = Left "Cant do operations with two columns"
    | otherwise = 
       case validateInput colName value parsedColumns of
        Left errorMessage -> Left errorMessage
        Right (col, validVal) ->
          case op of
              "=" -> Right (Equals col validVal)
              "<>" -> Right (NotEquals col validVal)
              "<=" -> Right (LessThan col validVal)
              "=>" -> Right (GreaterThan col validVal)
              _ -> Left "Invalid operator inside a condition"
parseToken _ _ = Left "Invalid condition"

matchesAnyInList :: String -> [ColumnName] -> Bool
matchesAnyInList input = any (\(ColumnName name _) -> name == input)

validateInput :: String -> String -> [ColumnName] -> Either ErrorMessage (ColumnName, String)
validateInput colName value parsedColumns
    | matchesAnyInList colName parsedColumns && not (matchesAnyInList value parsedColumns) = Right (ColumnName colName Nothing, value)
    | not (matchesAnyInList colName parsedColumns) && matchesAnyInList value parsedColumns = Right (ColumnName value Nothing, colName)

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement _ = Left "Not implemented: executeStatement"
