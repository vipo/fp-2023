{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement(..)
  )
where

import Data.Char (toLower)
import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..))
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
