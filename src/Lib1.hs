{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import Data.List (intercalate)
import DataFrame (DataFrame(..), Column(..), Value(..), ColumnType(..))
import InMemoryTables (TableName)
import Data.Char (toLower)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- 1) Return a data frame by its name from the provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName db tableName = 
    lookup (map toLower tableName) (map (\(name, df) -> (map toLower name, df)) db)

-- 2) Implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement "" = Left "Empty select statement"
parseSelectAllStatement statement =
  case words [toUpper x | x <- statement] of
    ["SELECT", "*", "FROM", tableName] -> validTableName tableName
      where
        originalTableName = last (words statement)
        validTableName tableNameToCheck
          | last tableNameToCheck == ';' && length tableNameToCheck > 1 = Right (removeLastChar originalTableName)
          | otherwise = Left "Invalid sql statement"
        removeLastChar :: String -> String
        removeLastChar [a] = []
        removeLastChar (x : xs) = x : removeLastChar xs
    _ -> Left "Invalid SQL Statement"

-- 3) Validate tables: check if columns match value types, if row sizes match columns, etc.
      validateDataFrame :: DataFrame -> Either ErrorMessage ()
      validateDataFrame (DataFrame [] _) = Left "DataFrame has no columns"
      validateDataFrame (DataFrame _ []) = Left "DataFrame has no rows"
      validateDataFrame (DataFrame cols rows) =
        if all validateRow rows
        then Right ()
        else Left "Types mismatch or row sizes do not match columns"
        where
          validateRow :: Row -> Bool
          validateRow row = length row == length cols && all validateColumnValue (zip cols row)
      
          validateColumnValue :: (Column, Value) -> Bool
          validateColumnValue (Column _ IntegerType, IntegerValue _) = True
          validateColumnValue (Column _ StringType, StringValue _) = True
          validateColumnValue (Column _ BoolType, BoolValue _) = True
          validateColumnValue (Column _ _, NullValue) = True  -- Assuming Null is allowed for all types
          validateColumnValue _ = False

-- 4) Render a given data frame as an ASCII-art table
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable terminalWidth (DataFrame columns rows) = 
    let header = map (\(Column name _) -> name) columns
        renderedRows = map (map renderValue) rows
        allRows = header : renderedRows
        colWidths = map (\col -> maximum $ map length col) (transpose allRows)
        totalWidth = sum colWidths + length colWidths + 1
        truncation = if totalWidth > fromIntegral terminalWidth then "..." else ""
        renderRow row = "|" ++ intercalate "|" (zipWith pad colWidths row) ++ "|"
        separator = "+" ++ intercalate "+" (map (\w -> replicate w '-') colWidths) ++ "+"
    in unlines $ [renderRow header, separator] ++ map renderRow renderedRows ++ [truncation]

pad :: Int -> String -> String
pad width str = str ++ replicate (width - length str) ' '

renderValue :: Value -> String
renderValue (IntegerValue i) = show i
renderValue (StringValue s) = s
renderValue (BoolValue b) = show b
renderValue NullValue = "NULL"

transpose :: [[a]] -> [[a]]
transpose ([]:_) = []
transpose x = (map head x) : transpose (map tail x)
