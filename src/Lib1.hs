module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    inMemoryDatabase,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

data ColumnType
  = IntegerType
  | StringType
  | BoolType
  deriving (Show, Eq)

data Column = Column
  { columnName :: String,
    columnType :: ColumnType
  }
  deriving (Show, Eq)

data Value
  = IntegerValue Integer
  | StringValue String
  | BoolValue Bool
  | NullValue
  deriving (Show, Eq)

type Row = [Value]

data DataFrame = DataFrame
  { columns :: [Column],
    rows :: [Row]
  }
  deriving (Show, Eq)

type ErrorMessage = String

type TableName = String

type Database = [(TableName, DataFrame)]

tableEmployees :: (TableName, DataFrame)
tableEmployees =
  ( "employees",
    DataFrame
      [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
      [ [IntegerValue 1, StringValue "Vi", StringValue "Po"],
        [IntegerValue 2, StringValue "Ed", StringValue "Dl"]
      ]
  )

tableInvalid1 :: (TableName, DataFrame)
tableInvalid1 =
  ( "invalid1",
    DataFrame
      [Column "id" IntegerType]
      [ [StringValue "1"]
      ]
  )

tableInvalid2 :: (TableName, DataFrame)
tableInvalid2 =
  ( "invalid2",
    DataFrame
      [Column "id" IntegerType, Column "text" StringType]
      [ [IntegerValue 1, NullValue],
        [IntegerValue 1]
      ]
  )

longString :: Value
longString =
  StringValue $
    unlines
      [ "Lorem ipsum dolor sit amet, mei cu vidisse pertinax repudiandae, pri in velit postulant vituperatoribus.",
        "Est aperiri dolores phaedrum cu, sea dicit evertitur no. No mei euismod dolorem conceptam, ius ne paulo suavitate.",
        "Vim no feugait erroribus neglegentur, cu sed causae aeterno liberavisse,",
        "his facer tantas neglegentur in. Soleat phaedrum pri ad, te velit maiestatis has, sumo erat iriure in mea.",
        "Numquam civibus qui ei, eu has molestiae voluptatibus."
      ]

tableLongStrings :: (TableName, DataFrame)
tableLongStrings =
  ( "long_strings",
    DataFrame
      [Column "text1" StringType, Column "text2" StringType]
      [ [longString, longString],
        [longString, longString],
        [longString, longString],
        [longString, longString],
        [longString, longString],
        [longString, longString]
      ]
  )

tableWithNulls :: (TableName, DataFrame)
tableWithNulls =
  ( "flags",
    DataFrame
      [Column "flag" StringType, Column "value" BoolType]
      [ [StringValue "a", BoolValue True],
        [StringValue "b", BoolValue True],
        [StringValue "b", NullValue],
        [StringValue "b", BoolValue False]
      ]
  )

renderSingleValue :: Value -> ColumnType -> Either ErrorMessage String
renderSingleValue (IntegerValue i) IntegerType = Right $ show i
renderSingleValue (StringValue s) StringType = Right s
renderSingleValue (BoolValue b) BoolType = Right $ show b
renderSingleValue NullValue _ = Right "NULL"
renderSingleValue v t = Left $ "Cannot render " ++ show v ++ " value as " ++ show t ++ " type"

inMemoryDatabase :: Database
inMemoryDatabase = [tableEmployees, tableInvalid1, tableInvalid2, tableLongStrings, tableWithNulls]

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
-- as ascii-art table, must use renderSingleValue to render
-- individual table cells (values) and should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable _ _ = error "renderDataFrameAsTable not implemented"
