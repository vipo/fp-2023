{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    loadFiles,
    getTime,
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (Column(..), DataFrame(..), Row)
import Data.Time (UTCTime)
type TableName = String
type FileContent = String
type ErrorMessage = String
type SQLQuery = String
type ColumnName = String

data SelectColumn
  = Now
  | TableColumn TableName ColumnName
  deriving (Show, Eq)

type SelectedColumns = [SelectColumn]
type SelectedTables = [TableName]

data ParsedStatement
  = SelectAll TableName
  | SelectColumns SelectedTables SelectedColumns
  | DeleteStatement TableName (Maybe WhereClause)
  | InsertStatement TableName [Column] Row
  | UpdateStatement TableName [Column] Row (Maybe WhereClause)
  deriving (Show, Eq)

data WhereClause
  = IsValueBool Bool TableName String
  | Conditions [Condition]
  deriving (Show, Eq)

data Condition
  = Equals String ConditionValue
  | GreaterThan String ConditionValue
  | LessThan String ConditionValue
  | LessthanOrEqual String ConditionValue
  | GreaterThanOrEqual String ConditionValue
  | NotEqual String ConditionValue
  deriving (Show, Eq)

data ConditionValue
  = StrValue String
  | IntValue Integer
  deriving (Show, Eq)

data StatementType = Select | Delete | Insert | Update 

data ExecutionAlgebra next
  = LoadFiles [TableName] ([FileContent] -> next)
  | ParseTables [FileContent] ([(TableName, DataFrame)] -> next)
  | UpdateTable (TableName, DataFrame) next
  | GetTime (UTCTime -> next)
  | GetStatementType SQLQuery (StatementType -> next)
  | ParseSql SQLQuery (ParsedStatement -> next)
  | GetTableNames ParsedStatement ([TableName] -> next)
  | DeleteRows ParsedStatement [(TableName, DataFrame)] ((TableName, DataFrame) -> next)
  | InsertRows ParsedStatement [(TableName, DataFrame)] ((TableName, DataFrame) -> next)
  | UpdateRows ParsedStatement [(TableName, DataFrame)] ((TableName, DataFrame) -> next)
  | GetSelectedColumns ParsedStatement [(TableName, DataFrame)] ([Column] -> next)
  | GetReturnTableRows ParsedStatement [(TableName, DataFrame)] ([Row] -> next)
  | GenerateDataFrame [Column] [Row] (DataFrame -> next)
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFiles :: [TableName] -> Execution [FileContent]
loadFiles names = liftF $ LoadFiles names id

parseTables :: [FileContent] -> Execution [(TableName, DataFrame)]
parseTables content = liftF $ ParseTables content id

updateTable :: (TableName, DataFrame) -> Execution ()
updateTable table = liftF $ UpdateTable table ()

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

getStatementType :: SQLQuery -> Execution StatementType
getStatementType query = liftF $ GetStatementType query id

parseSql :: SQLQuery -> Execution ParsedStatement
parseSql query = liftF $ ParseSql query id

getTableNames :: ParsedStatement -> Execution [TableName]
getTableNames statement = liftF $ GetTableNames statement id

deleteRows :: ParsedStatement -> [(TableName, DataFrame)] -> Execution (TableName, DataFrame)
deleteRows statement tables = liftF $ DeleteRows statement tables id

insertRows :: ParsedStatement -> [(TableName, DataFrame)] -> Execution (TableName, DataFrame)
insertRows statement tables = liftF $ InsertRows statement tables id

updateRows :: ParsedStatement -> [(TableName, DataFrame)] -> Execution (TableName, DataFrame)
updateRows statement tables = liftF $ UpdateRows statement tables id

getSelectedColumns :: ParsedStatement -> [(TableName, DataFrame)] -> Execution [Column]
getSelectedColumns statement tables = liftF $ GetSelectedColumns statement tables id

getReturnTableRows :: ParsedStatement -> [(TableName, DataFrame)] -> Execution [Row]
getReturnTableRows parsedStatement usedTables = liftF $ GetReturnTableRows parsedStatement usedTables id

generateDataFrame :: [Column] -> [Row] -> Execution DataFrame
generateDataFrame columns rows = liftF $ GenerateDataFrame columns rows id

executeSql :: SQLQuery -> Execution (Either ErrorMessage DataFrame)
executeSql statement = do
  statementType <- getStatementType statement
  parsedStatement <- parseSql statement
  
  tableNames <- getTableNames parsedStatement
  tableFiles <- loadFiles tableNames
  tables <- parseTables tableFiles
  
  _ <- case statementType of 
    Select -> do
      columns <- getSelectedColumns parsedStatement tables
      rows <- getReturnTableRows parsedStatement tables
      df <- generateDataFrame columns rows
      return $ Right df
    Delete -> do
      (name, df) <- deleteRows parsedStatement tables
      updateTable (name, df)
      return $ Right df 
    Insert -> do
      (name, df) <- insertRows parsedStatement tables
      updateTable (name, df)
      return $ Right df
    Update -> do
      (name, df) <- updateRows parsedStatement tables
      updateTable (name, df)
      return $ Right df

  return $ Left "errr"
