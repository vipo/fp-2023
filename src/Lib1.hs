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
import Data.Char (toLower)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName [] _ = Nothing
findTableByName ((name, table):rest) tableName =
  if map toLower name == map toLower tableName 
    then Just table 
    else findTableByName rest tableName

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement input
  | null input = Left "Empty input"
  | last input == ';' = parseSelectAllStatement (init input)
  | otherwise =
    case words (map toLower input) of
      ["select", "*", "from", table] -> Right table
      _ -> Left "Invalid SELECT statement"

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame (DataFrame columns rows) = validateRows rows
  where
    validateRow :: [Column] -> [Value] -> Either ErrorMessage ()
    validateRow [] [] = Right ()
    validateRow [] (_:_) = Left "Too many values"
    validateRow (_:_) [] = Left "Not enough values"
    validateRow (currentColumn:remainingColumns) (currentValue:remainingValues)
      | isValidType currentColumn currentValue = validateRow remainingColumns remainingValues
      | otherwise = Left "Incorrect type of the value"

    validateRows :: [Row] -> Either ErrorMessage ()
    validateRows [] = Right ()
    validateRows (currentRow:remainingRows) =
      case validateRow columns currentRow of
        Left error -> Left error
        Right _ -> validateRows remainingRows

    isValidType :: Column -> Value -> Bool
    isValidType _ NullValue = True
    isValidType (Column _ columnType) value =
      case checkValueType value of
        Right valueType -> valueType == columnType
        _ -> False

    checkValueType :: Value -> Either () ColumnType
    checkValueType (IntegerValue _) = Right IntegerType
    checkValueType (StringValue _) = Right StringType
    checkValueType (BoolValue _) = Right BoolType
    checkValueType _ = Left ()


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