{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..)
  )
where

import DataFrame
    ( DataFrame(..),
      Row,
      Column(..),
      ColumnType(..),
      Value(..),
      DataFrame)
import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame)
import Data.Time ( UTCTime )
import Lib2 (getType, getColumnName)
import Debug.Trace
import System.IO 

type TableName = String
type FileContent = String
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetTime (UTCTime -> next)
  -- feel free to add more constructors here
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

-------------------------------some JSON shit------------------------------------

toJSONtable :: (TableName, (DataFrame)) -> String
toJSONtable table = "{\"Table\":\"" ++ show(fst(table)) ++ "\",\"Columns\":[" ++ show(toJSONColumns(toColumnList(snd(table)))) ++ "],\"Rows\":[" ++ show(toJSONRows(toRowList(snd(table)))) ++"]}"

toJSONColumn :: Column -> String
toJSONColumn column = "{\"Name\":\"" ++ show(getColumnName(column)) ++ ",\"ColumnType\":\"" ++ show(getType(column)) ++ "\"}"

toJSONRowValue :: Value -> String
toJSONRowValue value = "{\"Value\":\"" ++ show(value) ++ "\"}"

---------------------------------------some get stuff----------------------------

toColumnList :: DataFrame -> [Column]
toColumnList (DataFrame col row)  = col

toRowList :: DataFrame -> [Row]
toRowList (DataFrame co row) = row

----------------------------------recursive stuff-------------------------------

toJSONColumns :: [Column] -> String
toJSONColumns (x:xs)
  |xs /= [] = toJSONColumn x ++ "," ++ toJSONColumns xs
  |otherwise = toJSONColumn x

toJSONRow :: Row -> String
--toJSONRow [] = []
toJSONRow (x:xs) 
  |xs /= [] = toJSONRowValue x ++ "," ++ toJSONRow xs
  |otherwise = toJSONRowValue x

toJSONRows :: [Row] -> String
toJSONRows (x:xs)
  |xs /= [] = "[" ++ toJSONRow x ++ "]," ++ toJSONRows xs
  |otherwise = "[" ++ toJSONRow x ++ "]" 

--------------------------------Files--------------------------------------------

toFilePath :: TableName -> FilePath
toFilePath tableName = "db/" ++ show(tableName) ++ ".txt"

writeTableToFile :: (TableName, DataFrame) -> IO () 
writeTableToFile table = writeFile (toFilePath(fst(table))) (toJSONtable(table))


---------------------------------------------------------------------------------

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
    return $ Left "implement me"