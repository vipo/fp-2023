{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..)
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame)
import Data.Time ( UTCTime )
import Lib1 (renderDataFrameAsTable, findTableByName, parseSelectAllStatement, checkTupleMatch, zipColumnsAndValues, checkRowSizes)
import Lib2 (parseStatement, executeStatement, ParsedStatement (..), ColumnName, Aggregate, AggregateFunction (..), And (..), SpecialSelect (..), AggregateList, Operand (..), Operator (..),  Condition (..), WhereSelect,validateDataFrame)
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

--parasyt parseri, pakeitus pavadinima

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
    return $ Left "implement me"

data ParsedStatement =
    SelectTables {
    table :: TableName,
    selectWhere :: Maybe WhereSelect
   }
   |SelectTablesAll {
    table :: TableName,
    selectWhere :: Maybe WhereSelect
   }
    deriving (Show, Eq)


