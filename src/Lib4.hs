module Lib4() where
import Lib3

type ColumnName = String

data ParsedStatement3 =
  SelectNow {}
  | DropTable {
    table :: TableName
  }
  | CreateTable {
    table :: TableName,
    newColumns :: [ColumnName]
  }
  | Insert {
    table :: TableName,
    columns :: Maybe SelectedColumns,
    values :: InsertedValues
  }
  | Update {
    table :: TableName,
    selectUpdate :: WhereSelect,
    selectWhere :: Maybe WhereSelect
  }
  | Delete {
    table :: TableName,
    conditions :: Maybe WhereSelect
  }
  | ShowTable {
    table :: TableName
   }
  | SelectAll {
    tables :: TableArray,
    selectWhere :: Maybe WhereSelect
   }
  |Select {
    selectQuery :: SpecialSelect2,
    tables :: TableArray,
    selectWhere :: Maybe WhereSelect
  }
  | ShowTables { }
    deriving (Show, Eq)