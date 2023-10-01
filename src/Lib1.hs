{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import InMemoryTables (TableName)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName _ _ = error "findTableByName not implemented"

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement _ = error "parseSelectAllStatement not implemented"

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame _ = error "validateDataFrame ot implemented"

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable _ (DataFrame [] _) = "The Columns list is empty."
renderDataFrameAsTable _ (DataFrame _ []) = "The Rows list is empty."
renderDataFrameAsTable maxWidth (DataFrame columns rows) = table
  where
    numCols = length columns
    colWidth = (fromIntegral maxWidth - 2) `div` numCols  -- Allow for separators '|'
    separator = "+" ++ concat (replicate numCols (replicate colWidth '-')) ++ "+\n"
    headerSeparator = "+" ++ concat (replicate numCols (replicate colWidth '=')) ++ "+\n"
    columnHeaders = separator ++ "|" ++ formatColumns (fromIntegral colWidth) columns ++ "|\n" ++ headerSeparator
    rowsWithSeparators = map (\row -> "|" ++ formatRow (fromIntegral colWidth) row ++ "|\n") rows ++ [separator]
    table = concat (columnHeaders : rowsWithSeparators)

formatColumns :: Integer -> [Column] -> String
formatColumns width = concatMap (formatColumn width)

formatColumn :: Integer -> Column -> String
formatColumn width (Column name _) = padString name width

formatRow :: Integer -> Row -> String
formatRow width = concatMap (formatValue width)

formatValue :: Integer -> Value -> String
formatValue width value = case value of
  NullValue -> format "NULL"
  IntegerValue n -> format (show n)
  StringValue s -> format s
  BoolValue False -> format "False"
  BoolValue True -> format "True"
  where
    format str = padString str width

padString :: String -> Integer -> String
padString s width
  | length s <= fromIntegral width = s ++ replicate (fromIntegral width - length s) ' '
  | otherwise = take (fromIntegral width - 2) s ++ ".."