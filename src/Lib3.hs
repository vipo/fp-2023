{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    loadFile,
    getTime,
    createDataFrame
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (Column(..), ColumnType(..), Value(..), DataFrame(..))
import Data.Time (UTCTime)

type TableName = String
type FileContent = String
type ErrorMessage = String
type SQLQuery = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetTime (UTCTime -> next)
  | ExecuteSQL SQLQuery (Either ErrorMessage DataFrame -> next)
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: SQLQuery -> Execution (Either ErrorMessage DataFrame)
executeSql "NOW();" = do
    currentTime <- getTime
    let df = createDataFrame currentTime
    return $ Right df
executeSql _ = return $ Left "wrong input"

createDataFrame :: UTCTime -> DataFrame
createDataFrame time =
    let timeColumn = Column "Time" StringType
        timeValue = StringValue $ show time
        row = [timeValue]
    in DataFrame [timeColumn] [row]
