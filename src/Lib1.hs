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
renderDataFrameAsTable width df@(DataFrame columns) =
  let getColumnWidths = map (maximum . map (length . show)) columns
      totalWidth = sum getColumnWidths + length columns - 1
      scale = fromIntegral width / fromIntegral totalWidth
      scaledWidths = map (\w -> round (fromIntegral w * scale)) getColumnWidths
      header = intercalate " | " $ zipWith padColumn columns scaledWidths
      separator = replicate (sum scaledWidths + length columns - 1) '-'
      rows = map (intercalate " | " . zipWith padColumn columns scaledWidths) (map getColumnValues (getRows df))
  in unlines (header : separator : rows)
  where
    getColumnValues (Column _ values) = map show values
    padColumn (Column name _) width = name ++ replicate (width - length name) ' '

getRows :: DataFrame -> [Row]
getRows (DataFrame columns) = transpose (map getColumnValues columns)

getColumnValues :: Column -> [Value]
getColumnValues (Column _ values) = values

transpose :: [[a]] -> [[a]]
transpose ([]:_) = []
transpose rows = (map head rows) : transpose (map tail rows)
renderDataFrameAsTable _ _ = error "renderDataFrameAsTable not implemented"
