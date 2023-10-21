{-# OPTIONS_GHC -Wno-overlapping-patterns #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    filterRowsByBoolColumn,
    ParsedStatement (..),
  )
where

import Data.Char (toLower)
import Data.List (elemIndex)
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row, Value (..))
import InMemoryTables (TableName, database)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]


data ParsedStatement 
    = ShowTableStmt TableName
    | ShowAllTablesStmt
    deriving (Show, Eq)

keywordMatch :: String -> String -> Bool
keywordMatch xs ys = map toLower xs == map toLower ys

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input 
  | last input /= ';' = Left "Unsupported or invalid statement"

  | keywordMatch "SHOW TABLES" (init input) = Right ShowAllTablesStmt
  | keywordMatch "SHOW TABLE" (unwords $ take 2 (words input)) && length (words input) > 2 = 
      Right $ ShowTableStmt (filter (/= ';') ((words input) !! 2))
  | otherwise = Left "Unsupported or invalid statement"

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (ShowTableStmt tableName) = 
  case lookup tableName database of
    Just df -> Right $ DataFrame [Column "Columns" StringType] (map (\(Column name _) -> [StringValue name]) (columns df))
    Nothing -> Left $ "Table " ++ tableName ++ " not found"
executeStatement ShowAllTablesStmt =
  Right $ DataFrame [Column "Tables" StringType] 
                   (map (\(name, _) -> [StringValue name]) database) 

columns :: DataFrame -> [Column]
columns (DataFrame cols _) = cols

-- Filter rows based on whether the specified column's value is TRUE or FALSE.

filterRowsByBoolColumn :: DataFrame -> Column -> Bool -> Either ErrorMessage DataFrame
filterRowsByBoolColumn (DataFrame cols rows) col bool
  | col `elem` cols && getColumnType col == BoolType = Right $ getRowsByBool bool rows
  | otherwise = Left "Dataframe does not contain column by specified name or column is not of type bool"
  where
    getColumnType :: Column -> ColumnType
    getColumnType (Column _ columnType) = columnType

    getRowsByBool :: Bool -> [Row] -> DataFrame
    getRowsByBool boolValue rows = DataFrame cols (filter (\row -> rowCellAtIndexIsBool boolValue row $ elemIndex col cols) rows)

    rowCellAtIndexIsBool :: Bool -> Row -> Maybe Int -> Bool
    rowCellAtIndexIsBool boolVal row index = case index of
      Just index -> row !! index == BoolValue boolVal
      Nothing -> False